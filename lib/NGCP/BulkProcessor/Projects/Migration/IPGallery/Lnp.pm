package NGCP::BulkProcessor::Projects::Migration::IPGallery::Lnp;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors

    $create_lnps_multithreading
    $create_lnps_numofthreads
    $create_lnp_block_txn
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
use NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers qw();
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
            if ($create_lnp_block_txn) {
                eval {
                    $context->{db}->db_begin();
                    foreach my $imported_lnp (@$records) {
                        $rownum++;
                        next unless _reset_context($context,$imported_lnp,$rownum);
                        _create_lnp($context);
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
            } else {
                foreach my $imported_lnp (@$records) {
                    $rownum++;
                    next unless _reset_context($context,$imported_lnp,$rownum);
                    eval {
                        $context->{db}->db_begin();
                        _create_lnp($context);
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
                }
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

    #NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::check_table();

}

sub _create_lnp {
    my ($context,$rownum) = @_;

    #eval {
    #    $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_lnp_numbers = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::findby_lnpproviderid_number($context->{db},
            undef, $context->{number});
        if ((scalar @$existing_lnp_numbers) == 0) {
            if ($context->{delta} eq
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta) {
                _info($context,"($context->{rownum}) " . 'lnp ' . $context->{number} . ' is deleted, and no lnp_number found');
            } else {
                _create_lnp_number($context);
            }
        } elsif ((scalar @$existing_lnp_numbers) >= 1) {
            _warn($context,"($context->{rownum}) " . 'multiple (' . (scalar @$existing_lnp_numbers) . ') existing lnp\'s ' . $context->{number} . ' found, processing each') if ((scalar @$existing_lnp_numbers) > 1);
            foreach my $existing_lnp_number (@$existing_lnp_numbers) {
                $context->{lnp_number_id} = $existing_lnp_number->{id};
                if ($context->{delta} eq
                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta) {
                    _info($context,"($context->{rownum}) " . 'lnp ' . $context->{number} . ' is deleted, but lnp_number found');
                    _delete_lnp_number($context);
                } else {
                    _info($context,"($context->{rownum}) " . 'lnp_number for lnp ' . $context->{number} . ' exists',1);
                    _update_lnp_number($context);
                }
            }
        }

        #if ($dry) {
        #    $context->{db}->db_rollback(0);
        #} else {
        #    $context->{db}->db_commit();
        #}

    #};
    #my $err = $@;
    #if ($err) {
    #    eval {
    #        $context->{db}->db_rollback(1);
    #    };
    #    die($err) if !$skip_errors;
    #}

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

    my $lrn_codes = [];
    eval {
        $lrn_codes = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::list_lrncodes(
        #{
        #    'NOT IN' => $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta
        #}
        );
    };
    $context->{lnp_provider_map} = {};
    foreach my $prefix (@$lrn_codes) {
        my $lnp_providers = [];
        eval {
            $lnp_providers = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::findby_prefix($prefix);
        };
        if ($@) {
            rowprocessingwarn(threadid(),"falling back to mr4.4.1 lnp_providers table definition ...",getlogger(__PACKAGE__));
            eval {
                $lnp_providers = NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers::findby_prefix($prefix);
            };
        };

        if ($@ or (scalar @$lnp_providers) != 1) {
            rowprocessingerror(threadid(),"cannot find a (unique) lnp carrier with prefix '$prefix'",getlogger(__PACKAGE__));
            $result = 0; #even in skip-error mode..
        } else {
            $context->{lnp_provider_map}->{$prefix} = $lnp_providers->[0];
        }
    }

    return $result;
}

sub _create_lnp_number {

    my ($context) = @_;

    $context->{lnp_number_id} = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::insert_row($context->{db},
        number => $context->{number},
        lnp_provider_id => $context->{lnp_provider}->{id},
    );
    _info($context,"($context->{rownum}) " . 'lnp_number ' . $context->{number} . ' created',1);

    return 1;

}

sub _update_lnp_number {

    my ($context) = @_;

    #effectively a no-op:
    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::update_row($context->{db},{
        id => $context->{lnp_number_id},
        number => $context->{number},
        lnp_provider_id => $context->{lnp_provider}->{id},
    });
    _info($context,"($context->{rownum}) " . 'lnp_number ' . $context->{number} . ' updated',1);

    return 1;

}

sub _delete_lnp_number {

    my ($context) = @_;

    #effectively a no-op:
    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::delete_row($context->{db},{
        id => $context->{lnp_number_id},
    });
    _info($context,"($context->{rownum}) " . 'lnp_number ' . $context->{number} . ' deleted');

    return 1;

}

sub _reset_context {

    my ($context,$imported_lnp,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{number} = $imported_lnp->{ported_number};
    $context->{lnp_provider} = $context->{lnp_provider_map}->{$imported_lnp->{lrn_code}};
    $context->{delta} = $imported_lnp->{delta};

    delete $context->{lnp_number_id};

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
