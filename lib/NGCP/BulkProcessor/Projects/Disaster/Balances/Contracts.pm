package NGCP::BulkProcessor::Projects::Disaster::Balances::Contracts;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
use DateTime qw();

use NGCP::BulkProcessor::Projects::Disaster::Balances::Settings qw(
    $dry
    $skip_errors

    $fix_contract_balance_gaps_multithreading
    $fix_contract_balance_gaps_numofthreads

    $fix_free_cash_multithreading
    $fix_free_cash_numofthreads
);
#$set_preference_bulk_numofthreads

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Dao::mr38::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr38::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::mr38::billing::billing_mappings qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::topup_log qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Array qw(array_to_map);
use NGCP::BulkProcessor::Calendar qw(current_local);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    fix_contract_balance_gaps
    fix_free_cash
);

sub fix_contract_balance_gaps {

    my $static_context = {};
    my $result = _fix_contract_balance_gaps_checks($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::mr38::billing::contracts::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $contract (@$records) {
                $rownum++;
                next unless _reset_fix_contract_balance_gaps_context($context,$contract,$rownum);
                _fix_contract_balance_gaps($context);
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
            destroy_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 0,
        multithreading => $fix_contract_balance_gaps_multithreading,
        numofthreads => $fix_contract_balance_gaps_numofthreads,
    ),$warning_count);
}

sub _fix_contract_balance_gaps {
    my ($context) = @_;

    eval {
        $context->{db}->db_begin();
        my $last_balance = undef;
        foreach my $contract_balance (sort NGCP::BulkProcessor::Dao::mr38::billing::contract_balances::sort_by_end_asc @{$context->{contract_balances}}) {
            #print "  " . $contract_balance->{id} . " " . $contract_balance->{_start} . ' ' . $contract_balance->{_end} . "\n";
            if (defined $last_balance) {
                my $gap_start = $last_balance->{_end}->clone->add(seconds => 1);
                my $gap_end = $contract_balance->{_start};
                my $date_comparison = DateTime->compare($gap_start, $gap_end);
                if ($date_comparison > 0) {
                    if ($skip_errors) {
                        _warn($context,"($context->{rownum}) " . 'contract balances overlap for contract id ' . $context->{contract}->{id} . ' detected: '.
                        $gap_start . ' - ' . $gap_end);
                    } else {
                        _error($context,"($context->{rownum}) " . 'contract balances overlap for contract id ' . $context->{contract}->{id} . ' detected: '.
                        $gap_start . ' - ' . $gap_end);
                    }
                } elsif ($date_comparison < 0) {
                    _info($context,"($context->{rownum}) " . 'contract balances gap for contract id ' . $context->{contract}->{id} . ' detected: '.
                        $gap_start . ' - ' . $gap_end);
                    _insert_contract_balances($context,$gap_start,$gap_end->clone->subtract(seconds => 1),$contract_balance);
                }
            }
            $last_balance = $contract_balance;
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
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'database error with contract id ' . $context->{contract}->{id} . ': ' . $err);
        } else {
            _error($context,"($context->{rownum}) " . 'database error with contract id ' . $context->{contract}->{id} . ': ' . $err);
        }
    }
}

sub _insert_contract_balances {
    my ($context,$gap_start,$gap_end,$contract_balance) = @_;

    my $start = $gap_start;
    my $last_end;
    my $end;
    while (($end = $start->clone->add(months => 1)->subtract(seconds => 1)) <= $gap_end) {
        my $billing_mapping = NGCP::BulkProcessor::Dao::mr38::billing::billing_mappings::findby_contractid_ts($context->{db},$context->{contract}->{id},$start)->[0];
        if (defined $billing_mapping) {
            #todo: check if billing profile is postpaid, has zero free_time and free_cash.
            #todo: contracts with profile packages defining intervals other than 1 month are not supported atm.
            #todo: dynamically choose mr38/4x contract_balance table dao.
            $last_end = $end;
            #_insert_contract_balances($context,$gap_start,$gap_end,$contract_balance,$billing_mapping);
            my $id = NGCP::BulkProcessor::Dao::mr38::billing::contract_balances::insert_row($context->{db},{
                contract_id => $context->{contract}->{id},
                start => $context->{db}->datetime_to_string($start),
                end => $context->{db}->datetime_to_string($end),
                cash_balance => 0,
                free_time_balance => 0,
            });
            _info($context,"($context->{rownum}) " . 'contract balance id ' . $id . ' for contract id ' . $context->{contract}->{id} . ' inserted: '.
                $start . ' - ' . $end);
        } else {
            if ($skip_errors) {
                _warn($context,"($context->{rownum}) " . 'no billing mapping for contract id ' . $context->{contract}->{id} . ', t = ' . $start . ' found ');
            } else {
                _error($context,"($context->{rownum}) " . 'no billing mapping for contract id ' . $context->{contract}->{id} . ', t = ' . $start . ' found ');
            }
        }
        $start = $end->clone->add(seconds => 1);
    }
    if (not defined $last_end or DateTime->compare($last_end, $gap_end) != 0) {
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'contract balances gap for contract id ' . $context->{contract}->{id} . ' cannot be filled with monthly intervals');
        } else {
            _error($context,"($context->{rownum}) " . 'contract balances gap for contract id ' . $context->{contract}->{id} . ' cannot be filled with monthly intervals');
        }
    }

}

sub _fix_contract_balance_gaps_checks {
    my ($context) = @_;

    my $result = 1;

    return $result;
}

sub _reset_fix_contract_balance_gaps_context {

    my ($context,$contract,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{contract} = $contract;

    $context->{contract_balances} = NGCP::BulkProcessor::Dao::mr38::billing::contract_balances::findby_contractid($context->{db},$context->{contract}->{id});

    return $result;

}











sub fix_free_cash {

    my $static_context = {};
    my $result = _fix_free_cash_checks($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::billing::contracts::process_free_cash_contracts(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $row (@$records) {
                $rownum++;
                next unless _reset_fix_free_cash_context($context,$row->[0],$rownum);
                _fix_free_cash($context);
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
            destroy_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 0,
        multithreading => $fix_free_cash_multithreading,
        numofthreads => $fix_free_cash_numofthreads,
    ),$warning_count);
}

sub _fix_free_cash {
    my ($context) = @_;

    eval {
        $context->{db}->db_begin();

        foreach my $balance (@{$context->{contract_balances}}) {


        }


        #my $last_balance = undef;
        #foreach my $contract_balance (sort NGCP::BulkProcessor::Dao::mr38::billing::contract_balances::sort_by_end @{$context->{contract_balances}}) {
        #    #print "  " . $contract_balance->{id} . " " . $contract_balance->{_start} . ' ' . $contract_balance->{_end} . "\n";
        #    if (defined $last_balance) {
        #        my $gap_start = $last_balance->{_end}->clone->add(seconds => 1);
        #        my $gap_end = $contract_balance->{_start};
        #        my $date_comparison = DateTime->compare($gap_start, $gap_end);
        #        if ($date_comparison > 0) {
        #            if ($skip_errors) {
        #                _warn($context,"($context->{rownum}) " . 'contract balances overlap for contract id ' . $context->{contract}->{id} . ' detected: '.
        #                $gap_start . ' - ' . $gap_end);
        #            } else {
        #                _error($context,"($context->{rownum}) " . 'contract balances overlap for contract id ' . $context->{contract}->{id} . ' detected: '.
        #                $gap_start . ' - ' . $gap_end);
        #            }
        #        } elsif ($date_comparison < 0) {
        #            _info($context,"($context->{rownum}) " . 'contract balances gap for contract id ' . $context->{contract}->{id} . ' detected: '.
        #                $gap_start . ' - ' . $gap_end);
        #            _insert_contract_balances($context,$gap_start,$gap_end->clone->subtract(seconds => 1),$contract_balance);
        #        }
        #    }
        #    $last_balance = $contract_balance;
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
            _warn($context,"($context->{rownum}) " . 'database error with contract id ' . $context->{contract}->{id} . ': ' . $err);
        } else {
            _error($context,"($context->{rownum}) " . 'database error with contract id ' . $context->{contract}->{id} . ': ' . $err);
        }
    }
}


sub _fix_free_cash_checks {
    my ($context) = @_;

    my $result = 1;

    $context->{now} = current_local();

    my $profile_count = 0;
    eval {
        ($context->{billing_profile_map},my $ids,my $domains) = array_to_map(NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findall(),
            sub { return shift->{id}; }, sub { return shift; }, 'first' );
        $profile_count = (scalar keys %{$context->{billing_profile_map}});
    };
    if ($@ or $profile_count == 0) {
        _error($context,"cannot find any billing profiles");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$profile_count billing profiles cached");
    }

    my $contract_free_cash_count = 0;
    eval {
        $contract_free_cash_count = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_free_cash();
    };
    if ($@ or $contract_free_cash_count == 0) {
        rowprocessingerror(threadid(),'no contracts with free cash billing profiles',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _reset_fix_free_cash_context {

    my ($context,$contract_id,$rownum) = @_;

    my $result = 0;

    $context->{rownum} = $rownum;

    # the goal is to adjust the latest contract_balances.cash_balance, so the
    # next contract_balance will start with the exact free cash expected:

    my $balance;
    my $mapping = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::findby_contractid_ts($context->{db},
            $contract_id,$context->{now})->[0];
    if ($mapping) {
        my $actual_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
        if (($actual_profile->{interval_free_cash} // 0.0) != 0.0) {
            $balance = shift sort NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::sort_by_end_desc
                @{NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::findby_contractid($context->{db},$contract_id)};
            my $carry_over_mode = xxx;
            if ($carry_over_mode ne $NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages::DISCARD_MODE) {
                $mapping = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::findby_contractid_ts($context->{db},
                    $contract_id,$balance->{_start})->[0];
                if ($mapping) {
                    my $billing_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
                    my $free_cash = $billing_profile->{interval_free_cash} // 0.0;
                    my $next_free_cash = $actual_profile->{interval_free_cash} // 0.0;
                    my $free_cash_carry_over = $next_free_cash;
                    if ($balance->{cash_balance_interval} < $free_cash) {
                        $free_cash_carry_over += $balance->{cash_balance_interval} - $free_cash;
                    }
                    $balance->{_cash_balance_delta} = $next_free_cash - ($balance->{cash_balance} + $free_cash_carry_over);
                    _info($context,"($context->{rownum}) " . 'contract id ' . $contract_id . ' cash balance delta: ' . $balance->{_cash_balance_delta},1);
                    $result = 1;
                } else {
                    if ($skip_errors) {
                        _warn($context,"($context->{rownum}) " . "no billing mapping at $balance->{_start} for contract id $contract_id");
                    } else {
                        _error($context,"($context->{rownum}) " . "no billing mapping at $balance->{_start} for contract id $contract_id");
                    }
                }
            } else {
                _info($context,"($context->{rownum}) " . 'contract id ' . $contract_id . ' skipped, is in ' . $carry_over_mode . ' carry over mode');
            }
        } else {
            _info($context,"($context->{rownum}) " . 'contract id ' . $contract_id . ' skipped, used a billing profile with free cash in the past');
        }
    } else {
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . "no billing mapping at $context->{now} for contract id $contract_id");
        } else {
            _error($context,"($context->{rownum}) " . "no billing mapping at $context->{now} for contract id $contract_id");
        }
    }

    return $result;

}

sub _check_insert_tables {

    #NGCP::BulkProcessor::Dao::mr38::provisioning::voip_usr_preferences::check_table();

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
