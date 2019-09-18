package NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Resellers;
use strict;

## no critic

use threads::shared qw();

#use Storable qw(dclone);

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Settings qw(
    $dry
    $skip_errors
    $report_filename

    $copy_contract_multithreading
    $copy_contract_numofthreads

);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
    fileerror
);


use NGCP::BulkProcessor::Dao::mr341::billing::resellers qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::ProjectConnectorPool qw(
    get_source_accounting_db
    get_source_billing_db
    get_source_provisioning_db
    get_source_kamailio_db
    destroy_all_dbs
);
#ping_all_dbs

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp stringtobool trim); #check_ipnet
#use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
#use NGCP::BulkProcessor::RandomString qw(createtmpstring);
use NGCP::BulkProcessor::Table qw(get_rowhash);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    copy_resellers_mr341
);

#my $db_lock :shared = undef;
#my $file_lock :shared = undef;

#my $default_barring = 'default';

#my $ccs_contact_identifier_field = 'gpp9';

my $contact_hash_field = 'gpp9';

sub copy_resellers_mr341 {

    my $context = {
        db => &get_xa_db(),
    };
    if (_copy_reseller_checks($context)) {
        my $source_dbs = {
            billing_db => \&get_source_billing_db,
            provisioning_db => \&get_source_provisioning_db,
        };
        foreach my $reseller (@{NGCP::BulkProcessor::Dao::mr341::billing::resellers::source_findall($source_dbs)}) {
            next unless _init_copy_reseller_context($context,$reseller);
            _copy_reseller($context);
        }
    }


    #my $static_context = { now => timestamp(), _rowcount => undef };
    #my $result = _copy_contracts_checks($static_context);
    #
    #destroy_all_dbs();
    #my $warning_count :shared = 0;
    #return ($result && NGCP::BulkProcessor::Dao::mr38::billing::contracts::source_process_records(
    #    source_dbs => {
    #        billing_db => \&get_source_billing_db,
    #        provisioning_db => \&get_source_provisioning_db,
    #    },
    #    static_context => $static_context,
    #    process_code => sub {
    #        my ($context,$records,$row_offset) = @_;
    #        #ping_all_dbs();
    #        $context->{_rowcount} = $row_offset;
    #        foreach my $record (@$records) {
    #            $context->{_rowcount} += 1;
    #            next unless _copy_contract($context,$record);
    #        #    push(@report_data,_get_report_obj($context));
    #        }
    #        #cleanup_aig_sequence_ids($context);
    #        return 1;
    #    },
    #    init_process_context_code => sub {
    #        my ($context)= @_;
    #        $context->{db} = &get_xa_db();
    #        $context->{error_count} = 0;
    #        $context->{warning_count} = 0;
    #    },
    #    uninit_process_context_code => sub {
    #        my ($context)= @_;
    #        undef $context->{db};
    #        destroy_all_dbs();
    #        {
    #            lock $warning_count;
    #            $warning_count += $context->{warning_count};
    #        }
    #        #{
    #        #    lock %nonunique_contacts;
    #        #    foreach my $sip_username (keys %{$context->{nonunique_contacts}}) {
    #        #        $nonunique_contacts{$sip_username} = $context->{nonunique_contacts}->{$sip_username};
    #        #    }
    #        #}
    #    },
    #    multithreading => $copy_contract_multithreading,
    #    numofthreads => $copy_contract_numofthreads,
    #),$warning_count,);

}

sub _copy_reseller {
    my ($context) = @_;

    eval {
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_resellers = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name_states(
            $context->{db},
            $context->{reseller}->{name},
            { 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE}
        );

        if ((scalar @$existing_resellers) > 0) {
            _warn($context,"reseller '$context->{reseller}->{name}' already exists, skipping");
        } else {
            _create_contact($context);
            _create_contract($context);
        }

        #if ($context->{reseller}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::insert_row(
        #    $context->{db},
        #    $context->{reseller},
        #)) {
        #    _info($context,"reseller '$context->{reseller}->{name}' created");
        #}


        if ($dry) {
            $context->{db}->db_rollback(0);
        } else {
            $context->{db}->db_commit();
        }

    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_rollback(1);
        };
        if ($skip_errors) {
            _warn($context, $err);
        } else {
            _error($context, $err);
        }
    }

    return 1;

}

sub _create_contact {

    my ($context) = @_;

    my $existing_contacts = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_fields($context->{db},{
        $contact_hash_field => $context->{contact}->{$contact_hash_field},
    });
    if ((scalar @$existing_contacts) > 0) {
        my $existing_contact = $existing_contacts->[0];
        if ((scalar @$existing_contacts) > 1) {
            _warn($context,(scalar @$existing_contacts) . " existing contacts found, using first contact id $existing_contact->{id}");
        } else {
            _info($context,"existing contact id $existing_contact->{id} found",1);
        }
        $context->{contract}->{contact_id} = $existing_contact->{id};
    } else {
        $context->{contract}->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
            $context->{contact},
        );
        _info($context,"contact id $context->{contract}->{contact_id} created",1);
    }

    return 1;

}

sub _create_contract {

    my ($context) = @_;

    $context->{contract}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
        $context->{contract},
    );
    _info($context,"contract id $context->{contract}->{id} created",1);

    return 1;

}

sub _init_copy_reseller_context {

    my ($context,$reseller) = @_;

    my $result = 1;

    #$context->{reseller} = dclone($reseller);
    $context->{reseller} = { %$reseller };
    delete $context->{reseller}->{id};
    $context->{billing_profiles} = delete $context->{reseller}->{billing_profiles};
    $context->{contract} = { %{delete $context->{reseller}->{contract}} };
    delete $context->{contract}->{id};
    $context->{contract}->{product_id} = $context->{voip_reseller_account_product}->{id};
    $context->{billing_mappings} = delete $context->{contract}->{billing_mappings};
    $context->{contract_balances} = delete $context->{contract}->{contract_balances};
    delete $context->{contract}->{voip_subscribers};
    $context->{contact} = { %{delete $context->{contract}->{contact}} };
    delete $context->{contact}->{id};
    delete $context->{contact}->{reseller_id};
    #$result = 0 unless $contract->{contact}->{reseller};
    my @contact_fields = grep { $_ ne 'id' and $_ ne $contact_hash_field and not ref $context->{contact}->{$_}; } sort keys %{$context->{contact}};
    $context->{contact}->{$contact_hash_field} = get_rowhash([@{$context->{contact}}{@contact_fields}]);

    if ($context->{reseller}->{status} eq $NGCP::BulkProcessor::Dao::mr341::billing::resellers::TERMINATED_STATE) {
        _info($context,"skipping terminated reseller '$context->{reseller}->{name}'");
        $result = 0;
    }



    return $result;

}

sub _copy_reseller_checks {
    my ($context) = @_;

    my $result = 1;

    eval {
        $context->{voip_reseller_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::products::VOIP_RESELLER_ACCOUNT_HANDLE)->[0];
    };
    if ($@ or not defined $context->{voip_reseller_account_product}) {
        rowprocessingerror(threadid(),"cannot find $NGCP::BulkProcessor::Dao::Trunk::billing::products::VOIP_RESELLER_ACCOUNT_HANDLE product",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"$NGCP::BulkProcessor::Dao::Trunk::billing::products::VOIP_RESELLER_ACCOUNT_HANDLE product found",getlogger(__PACKAGE__));
    }


    return $result;
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;
