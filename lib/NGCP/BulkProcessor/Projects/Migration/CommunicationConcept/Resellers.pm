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

    $copy_billing_fees_multithreading
    $copy_billing_fees_numofthreads

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
use NGCP::BulkProcessor::Dao::mr341::billing::billing_zones qw();
use NGCP::BulkProcessor::Dao::mr341::billing::billing_fees qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();

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

    my $context = {};
    my $result = 1;
    if (_copy_reseller_checks($context)) {
        $context->{source_dbs} = {
            billing_db => \&get_source_billing_db,
            provisioning_db => \&get_source_provisioning_db,
        };
        foreach my $reseller (@{NGCP::BulkProcessor::Dao::mr341::billing::resellers::source_findall($context->{source_dbs})}) {
            unless (_init_copy_reseller_context($context,$reseller)) {
                $result = 0; next;
            }
            unless (_copy_reseller($context)) {
                $result = 0; next;
            }
        }
        $result &= _copy_billing_mappings($context);
        $result &= _copy_billing_fees_mr341($context);
    } else {
        $result = 0;
    }
    return $result;

}

sub _copy_reseller {
    my ($context) = @_;

    my $result = 0;
    eval {
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_resellers = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name_states(
            $context->{db},
            $context->{reseller}->{name},
            #{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE}
        );

        if ((scalar @$existing_resellers) > 0) {
            _warn($context,"reseller '$context->{reseller}->{name}' already exists, skipping");
        } else {
            _create_contact($context);
            _create_contract($context);
            _create_reseller($context);
            _create_billing_profiles($context);
            _create_domains($context);

            $result = 1;
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
            if ($result) {
                $context->{deferred_billing_mappings}->{$context->{reseller}->{contract_id}} = $context->{billing_mappings};
                @{$context->{billing_profile_id_map}}{keys %{$context->{_billing_profile_id_map}}} = values %{$context->{_billing_profile_id_map}};
            }
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

    return $result;

}

sub _copy_billing_mappings {
    my ($context) = @_;

    return 1 if $dry;

    my $result = 0;
    eval {
        $context->{db}->db_begin();
        foreach my $contract_id (keys %{$context->{deferred_billing_mappings}}) {
            my @mappings = ();
            foreach my $bm (@{$context->{deferred_billing_mappings}->{$contract_id}}) {
                my $mapping = { %$bm };
                #$mapping->{contract_id} = $contract_id;
                $mapping->{billing_profile_id} = $context->{billing_profile_id_map}->{$mapping->{billing_profile_id}};
                push(@mappings,$mapping);
            }
            NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule::append_billing_mappings($context->{db},
                $contract_id,
                \@mappings,
            );
            _info($context,"system contract id $contract_id billing mappings created");
        }
        $context->{db}->db_commit();
        $result = 1;
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

    return $result;
}

sub _copy_billing_zones_mr341 {
    my ($context) = @_;

    #return 1 if $dry;

    my $result = 0;
    eval {
        $context->{db}->db_begin();
        my $billing_zone_id_map = {};
        foreach my $bz (@{NGCP::BulkProcessor::Dao::mr341::billing::billing_zones::source_findby_billingprofileid(
                $context->{source_dbs},
                $context->{old_billing_profile_id})}) {
            my $billing_zone = { %$bz };
            delete $billing_zone->{id};
            $billing_zone->{billing_profile_id} = $context->{new_billing_profile_id};
            $billing_zone->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones::insert_row($context->{db},
                $billing_zone,
            );
            _info($context,"billing_zone id $billing_zone->{id} created",1);
            $billing_zone_id_map->{$bz->{id}} = $billing_zone->{id};
        }
        $context->{db}->db_commit();
        _info($context,"billing profile id $context->{new_billing_profile_id} billing zones created",0);
        $context->{billing_zone_id_map} = $billing_zone_id_map;
        $result = 1;
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

    return $result;
}

sub _copy_billing_fees_mr341 {
    my ($reseller_context) = @_;
    undef $reseller_context->{db};
    destroy_all_dbs();
    my $context = {
        source_dbs => $reseller_context->{source_dbs},
    };
    my $result = 1;
    my $warning_count :shared = 0;
    foreach my $old_billing_profile_id (keys %{$reseller_context->{billing_profile_id_map}}) {
        $context->{old_billing_profile_id} = $old_billing_profile_id;
        $context->{new_billing_profile_id} =  $reseller_context->{billing_profile_id_map}->{$old_billing_profile_id};
        $context->{billing_zone_id_map} = {};
        $context->{db} = &get_xa_db();
        unless (_copy_billing_zones_mr341($context)) {
            $result = 0;
            next;
        }
        undef $context->{db};
        destroy_all_dbs();
        $result &= NGCP::BulkProcessor::Dao::mr341::billing::billing_fees::source_process_records(
            source_dbs => $reseller_context->{source_dbs},
            static_context => $context,
            billing_profile_id => $old_billing_profile_id,
            process_code => sub {
                my ($context,$records,$row_offset) = @_;
                eval {
                    $context->{db}->db_begin();
                    foreach my $bf (@$records) {
                        my $billing_fee = { %$bf };
                        delete $billing_fee->{id};
                        $billing_fee->{billing_profile_id} = $context->{new_billing_profile_id};
                        $billing_fee->{billing_zone_id} = $context->{billing_zone_id_map}->{$billing_fee->{billing_zone_id}};
                        $billing_fee->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees::insert_row($context->{db},
                            $billing_fee,
                            1, #ignore duplicates
                        );
                        _info($context,"billing_fee id $billing_fee->{id} created",1);
                    }
                    $context->{db}->db_commit();
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
                    return 0;
                } else {
                    return 1;
                }
            },
            init_process_context_code => sub {
                my ($context)= @_;
                $context->{db} = &get_xa_db();
                $context->{error_count} = 0;
                $context->{warning_count} = 0;
            },
            uninit_process_context_code => sub {
                my ($context)= @_;
                undef $context->{db};
                destroy_all_dbs();
                {
                    lock $warning_count;
                    $warning_count += $context->{warning_count};
                }
            },
            destroy_reader_dbs_code => \&destroy_all_dbs,
            multithreading => $copy_billing_fees_multithreading,
            numofthreads => $copy_billing_fees_numofthreads,
        );
        _info($context,"billing profile id $context->{new_billing_profile_id} billing fees created",0);
    }
    return $result;
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
            _info($context,"existing system contact id $existing_contact->{id} found",0);
        }
        $context->{contract}->{contact_id} = $existing_contact->{id};
    } else {
        $context->{contract}->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
            $context->{contact},
        );
        _info($context,"system contact id $context->{contract}->{contact_id} created",0);
    }

    return 1;

}

sub _create_contract {

    my ($context) = @_;

    $context->{reseller}->{contract_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
        $context->{contract},
    );
    _info($context,"system contract id $context->{reseller}->{contract_id} created",0);

    foreach my $cb (@{$context->{contract_balances}}) {
        my $contract_balance = { %$cb };
        delete $contract_balance->{id};
        delete $contract_balance->{invoice_id};
        $contract_balance->{contract_id} = $context->{reseller}->{contract_id};
        $contract_balance->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
            $contract_balance,
        );
    }
    _info($context,"system contract id $context->{reseller}->{contract_id} contract balances created",0);

    #$context->{deferred_billing_mappings}->{$context->{reseller}->{contract_id}} = $context->{billing_mappings};

    return 1;

}

sub _create_reseller {

    my ($context) = @_;

    $context->{reseller_id} = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::insert_row($context->{db},
        $context->{reseller},
    );
    _info($context,"reseller id $context->{reseller_id} '$context->{reseller}->{name}' created",0);

    return 1;

}

sub _create_billing_profiles {

    my ($context) = @_;

    foreach my $bp (@{$context->{billing_profiles}}) {
        my $billing_profile = { %$bp };
        delete $billing_profile->{id};
        $billing_profile->{reseller_id} = $context->{reseller_id};
        $billing_profile->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::insert_row($context->{db},
            $billing_profile,
        );
        _info($context,"billing_profile id $billing_profile->{id} created",0);
        $context->{_billing_profile_id_map}->{$bp->{id}} = $billing_profile->{id};
    }

    return 1;

}

sub _create_domains {

    my ($context) = @_;

    foreach my $d (@{$context->{domains}}) {
        my $domain = { %$d };
        delete $domain->{id};
        $domain->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::insert_row($context->{db},
            $domain,
        );
        NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers::insert_row($context->{db},
            domain_id => $domain->{id},
            reseller_id => $context->{reseller_id},
        );
        _info($context,"domain id $domain->{id} '$domain->{domain}' created",0);
    }

    return 1;

}

sub _init_copy_reseller_context {

    my ($context,$reseller) = @_;

    my $result = 1;

    $context->{deferred_billing_mappings} //= {};
    $context->{billing_profile_id_map} //= {};
    $context->{db} //= &get_xa_db();

    $context->{reseller_id} = undef;
    $context->{_billing_profile_id_map} = {};

    #$context->{reseller} = dclone($reseller);
    $context->{reseller} = { %$reseller };
    delete $context->{reseller}->{id};
    $context->{billing_profiles} = delete $context->{reseller}->{billing_profiles};
    $context->{contract} = { %{delete $context->{reseller}->{contract}} };
    $context->{domains} = delete $context->{reseller}->{domains};
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

    #if ($context->{reseller}->{status} eq $NGCP::BulkProcessor::Dao::mr341::billing::resellers::TERMINATED_STATE) {
    #    _info($context,"skipping terminated reseller '$context->{reseller}->{name}'");
    #    $result = 0;
    #}



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
        _error($context,"cannot find $NGCP::BulkProcessor::Dao::Trunk::billing::products::VOIP_RESELLER_ACCOUNT_HANDLE product");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$NGCP::BulkProcessor::Dao::Trunk::billing::products::VOIP_RESELLER_ACCOUNT_HANDLE product found");
    }

    destroy_all_dbs();

    return $result;
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    }
}

1;
