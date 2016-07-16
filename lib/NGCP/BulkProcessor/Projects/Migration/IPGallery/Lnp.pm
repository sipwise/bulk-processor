package NGCP::BulkProcessor::Projects::Migration::IPGallery::Lnp;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors

    $create_lnps_multithreading
    $create_lnps_numofthreads
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    create_lnps
);

sub create_lnps {

    my $static_context = {};
    my $result = _create_lnps_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_lnp (@$records) {
                $rownum++;
                next unless _create_lnp($context,$imported_lnp,$rownum);
            }

            #return 0;
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            # below is not mandatory..
            _check_insert_tables();
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
        load_recursive => 0,
        multithreading => $create_lnps_multithreading,
        numofthreads => $create_lnps_numofthreads,
    ),$warning_count);
}


sub _check_insert_tables {

    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::check_table();

}

sub _create_lnp {
    my ($context,$imported_lnp,$rownum) = @_;

    return 0 unless _reset_context($context,$imported_lnp,$rownum);

    eval {
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states($context->{db},
            $context->{billing_domain}->{id},$context->{username},{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE});
        if ((scalar @$existing_billing_voip_subscribers) == 0) {

            if ($imported_subscriber->{delta} eq
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {
                _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, and no active subscriber found');
            } else {

                my $existing_provisioning_voip_dbalias = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_domainid_username($context->{db},
                    $context->{provisioning_voip_domain}->{id},$context->{cli});

                if (not defined $existing_provisioning_voip_dbalias) {
                    _create_contact($context);
                    _create_contract($context);
                    _create_subscriber($context);
                    _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' successfully provisioned',1);


                } else {
                    _warn($context,"($context->{rownum}) " . 'existing provisioning voip_dbalias with username ' . $context->{cli} . ' found, skipping');
                }
            }
        } elsif ((scalar @$existing_billing_voip_subscribers) == 1) {
            my $existing_billing_voip_subscriber = $existing_billing_voip_subscribers->[0];
            if ($imported_subscriber->{delta} eq
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {

                _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, but active subscriber found');

                if (_terminate_subscriber($context,$existing_billing_voip_subscriber->{id})) {
                    _terminate_contract($context,$existing_billing_voip_subscriber->{contract_id});
                }

            } else {
                if ($context->{userpassworddelta} eq
                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta) {

                    _info($context,"($context->{rownum}) " . 'existing billing subscriber with username ' . $context->{username} . ' and updated password found (re-provisioned)');

                    if (_terminate_subscriber($context,$existing_billing_voip_subscriber->{id})) {
                        if (_terminate_contract($context,$existing_billing_voip_subscriber->{contract_id})) {
                            if ($dry) {
                                _create_contact($context);
                                _create_contract($context);
                                eval {
                                    _create_subscriber($context);
                                };
                                if ($@) {
                                    _info($context,"($context->{rownum}) " . 'expected error ' . $@ . ' while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode',1);
                                } else {
                                    if ($skip_errors) {
                                        _warn($context,"($context->{rownum}) " . 'expected error while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode missing');
                                    } else {
                                        _error($context,"($context->{rownum}) " . 'expected error while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode missing');
                                    }
                                }
                            } else {
                                _create_contact($context);
                                _create_contract($context);
                                _create_subscriber($context);
                                _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' successfully re-provisioned');
                            }
                        }
                    }
                } else {
                    _info($context,"($context->{rownum}) " . 'existing billing subscriber with username ' . $context->{username} . ' and unchanged password found, skipping',1);
                }
            }
        } else {
            _warn($context,"($context->{rownum}) " . 'multiple (' . (scalar @$existing_billing_voip_subscribers) . ') existing billing subscribers with username ' . $context->{username} . ' found, skipping');
        }

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
        die($err) if !$skip_errors;
    }

    return 1;

}

sub _create_lnps_checks {
    my ($context) = @_;

    my $result = 1;
    my $lnpcount = 0;
    eval {
        $lnpcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_lrncode_portednumber();
    };
    if ($@ or $lnpcount == 0) {
        rowprocessingerror(threadid(),'please import lnps first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _create_xx {

    my ($context) = @_;



    return 1;

}

sub _create_yy {

    my ($context) = @_;


    return 1;

}


sub _reset_context {

    my ($context,$imported_lnp,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{blah} = $imported_lnp->blah();

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
