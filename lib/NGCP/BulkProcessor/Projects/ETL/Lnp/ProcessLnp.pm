package NGCP::BulkProcessor::Projects::ETL::Lnp::ProcessLnp;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::ETL::Lnp::Settings qw(

    $skip_errors
    
    $create_lnp_multithreading
    $create_lnp_numofthreads   

    $delete_lnp_multithreading
    $delete_lnp_numofthreads
    
    $ignore_lnp_numbers_unique
    $lnp_numbers_single_row_txn
    
    $lnp_numbers_batch_delete
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

use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers qw();

use NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::ETL::Lnp::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
    ping_all_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid); 

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    create_lnp_numbers
    delete_lnp_numbers
);

sub create_lnp_numbers {

    my $static_context = {};
    my $result = _create_lnp_numbers_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    my $result = $result && NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::process_records(
        static_context => $static_context,
        deltas => $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::added_delta,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            foreach my $row (@$records) {
                my $lnp = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp->new($row);
                my $lnp_number = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers->new({ %$lnp });
                my $lnp_provider = $context->{carrier_map}->{
                    $lnp->carrier_hash()
                };
                $lnp_number->{lnp_provider_id} = $lnp_provider->{id};
            
                my %r = %$lnp_number; my @row_ext = @r{@NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::fieldnames};
                
                push(@{$context->{lnp_numbers}},\@row_ext);
                if ($lnp_numbers_single_row_txn and (scalar @{$context->{lnp_numbers}}) > 0) {
                    while (defined (my $lnp_number = shift @{$context->{lnp_numbers}})) {
                        if ($skip_errors) {
                            eval { _insert_lnp_numbers($context,[$lnp_number]); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_lnp_numbers($context,[$lnp_number]);
                        }
                    }
                }       
            }
            
            if (not $lnp_numbers_single_row_txn and (scalar @{$context->{lnp_numbers}}) > 0) {
                if ($skip_errors) {
                    eval { _insert_lnp_numbers($context,$context->{lnp_numbers}); };
                    _warn($context,$@) if $@;
                } else {
                    _insert_lnp_numbers($context,$context->{lnp_numbers});
                }
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            $context->{lnp_numbers} = [];
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
        multithreading => $create_lnp_multithreading,
        numofthreads => $create_lnp_numofthreads,
    );
    
    return ($result,$warning_count);

}


sub _create_lnp_numbers_checks {

    my $context = shift;
    my $result = 1;
    
    $context->{carrier_map} = {};
    my $carriers = [];
    eval {
        $carriers = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::find_carriers_by_delta($NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::added_delta);   
    };
    if ($@) {
        $result = 0; #even in skip-error mode..
    } else {
        foreach my $carrier (@$carriers) {
            my $lp = {
                name => $carrier->{carrier_name},
                prefix => ($carrier->{carrier_prefix} // ''),
                authoritative => ($carrier->{authoritative} // 0),
                skip_rewrite => ($carrier->{skip_rewrite} // 0),
            };
            my $lnp_provider = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::findby_nameprefixauthoritativeskiprewrite(
                $lp->{name},
                $lp->{prefix},
                $lp->{authoritative},
                $lp->{skip_rewrite},
            )->[0];
            if ($lnp_provider) {
                processing_info(threadid(),"lnp provider '$lnp_provider->{name}' found",getlogger(__PACKAGE__));
            } else {
                $lnp_provider = { %$lp };
                $lnp_provider->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::insert_row(undef,$lp);
                processing_info(threadid(),"lnp provider '$lnp_provider->{name}' created",getlogger(__PACKAGE__));
            }
            $context->{carrier_map}->{
                $carrier->carrier_hash()
            } = $lnp_provider;
        }
    }

    return $result;
}


sub _insert_lnp_numbers {
    my ($context,$lnp_numbers) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::getinsertstatement($ignore_lnp_numbers_unique),
    );
    eval {
        $context->{db}->db_do_rowblock($lnp_numbers);
        $context->{db}->db_finish();
    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_finish(1);
        };
        die($err);
    }
}


sub delete_lnp_numbers {

    my $static_context = {};
    my $result = 1; 

    destroy_all_dbs();
    my $warning_count :shared = 0;
    my $result = $result && NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::process_records(
        static_context => $static_context,
        deltas => $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::deleted_delta,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            foreach my $row (@$records) {
                my $lnp = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp->new($row);
                push(@{$context->{numbers}},$lnp->{number});
                if (not $lnp_numbers_batch_delete and (scalar @{$context->{numbers}}) > 0) {
                    while (defined (my $number = shift @{$context->{numbers}})) {
                        if ($skip_errors) {
                            eval {
                                NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::delete_numbers($context->{db},$number);
                            };
                        } else {
                            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::delete_numbers($context->{db},$number);
                        }
                    }
                }       
            }
            
            if ($lnp_numbers_batch_delete and (scalar @{$context->{numbers}}) > 0) {
                if ($skip_errors) {
                    eval {
                        NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::delete_numbers($context->{db},{
                            'IN' => $context->{numbers},
                        });
                    };
                } else {
                    NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::delete_numbers($context->{db},{
                        'IN' => $context->{numbers},
                    });
                }
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            $context->{numbers} = [];
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
        multithreading => $create_lnp_multithreading,
        numofthreads => $create_lnp_numofthreads,
    ) && _delete_lnp_providers($static_context);
    
    return ($result,$warning_count);

}


sub _delete_lnp_providers {

    my $context = shift;
    my $result = 1;
    
    my $carriers = [];
    eval {
        $carriers = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::find_carriers_by_delta($NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::deleted_delta);   
    };
    if ($@) {
        $result = 0; #even in skip-error mode..
    } else {
        foreach my $carrier (@$carriers) {
            my $lp = {
                name => $carrier->{carrier_name},
                prefix => ($carrier->{carrier_prefix} // ''),
                authoritative => ($carrier->{authoritative} // 0),
                skip_rewrite => ($carrier->{skip_rewrite} // 0),
            };
            foreach my $lnp_provider (@{NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::findby_nameprefixauthoritativeskiprewrite(
                    $lp->{name},
                    $lp->{prefix},
                    $lp->{authoritative},
                    $lp->{skip_rewrite},
                )}) {
                NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::delete_row(undef,$lnp_provider);
                processing_info(threadid(),"lnp provider '$lnp_provider->{name}' removed",getlogger(__PACKAGE__));
            }
        }
    }

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
