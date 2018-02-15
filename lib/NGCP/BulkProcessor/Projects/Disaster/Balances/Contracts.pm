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
use NGCP::BulkProcessor::Dao::Trunk::billing::topup_log qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid stringtobool);
use NGCP::BulkProcessor::Array qw(array_to_map);
use NGCP::BulkProcessor::Calendar qw(is_infinite_future current_local);

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

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id($contract_id);
    my $contract_create = $context->{db}->datetime_from_string((
        $context->{contract}->{create_timestamp} ne '0000-00-00 00:00:00' ?
        $context->{contract}->{create_timestamp} : $context->{contract}->{modify_timestamp}),'local');
    #$context->{contract_create} = $contract_create;

    #my $prepaid = 0;
    #my $last_balance_profile;
    my @free_cash_balances = ();
    my @balances = ();
    foreach my $balance (sort NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::sort_by_end_asc
        @{NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::findby_contractid($context->{db},$contract_id)}) {

        my $s = ((DateTime->compare($contract_create, $balance->{_start}) > 0) ? $contract_create : $balance->{_start});
        #my $is_actual = ((is_infinite_future($balance->{_end}) || DateTime->compare($balance->{_end}, $context->{now}) >= 0) ? 1 : 0);

        my $billing_profile;
        if (my $mapping = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::findby_contractid_ts($context->{db},
            $contract_id,$s)->[0]) {
            $billing_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
            $balance->{_profile} = $billing_profile;
            push(@free_cash_balances,$balance) if $billing_profile->{interval_free_cash} > 0.0;
            #$last_balance_profile = $billing_profile;
            #$prepaid = stringtobool($billing_profile->{prepaid}) unless defined $prepaid;
        } else {
            if ($skip_errors) {
                _warn($context,"($context->{rownum}) " . "no billing mapping at $s for contract id $contract_id");
            } else {
                _error($context,"($context->{rownum}) " . "no billing mapping at $s for contract id $contract_id");
            }
            $result = 0;
        }

        my $topup_sum = 0.0;
        foreach my $topup (@{NGCP::BulkProcessor::Dao::Trunk::billing::topup_log::findby_contractbalanceid(
            $balance->{id},undef)}) {
            $topup_sum += $topup->{amount} if (defined $topup->{amount} and $topup->{outcome} eq $NGCP::BulkProcessor::Dao::Trunk::billing::topup_log::OK_OUTCOME);
        }
        #$balance->{_topup_sum} = $topup_sum;
        _info($context,"($context->{rownum}) " . "contract id $contract_id topup sum $topup_sum ($s to $balance->{_end})") if $topup_sum > 0.0;

        if ((scalar @balances) > 0) {

        } else {

        }


        push(@balances,$balance);

    }

    my $prepaid = 0;
    my $actual_profile;
    my $mapping = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::findby_contractid_ts($context->{db},
            $contract_id,$context->{now})->[0];
    if ($mapping) {
        $actual_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
        $prepaid = stringtobool($actual_profile->{prepaid});
    }

    $context->{contract_balances} = \@balances;
    #$context->{prepaid} = $prepaid;
    #$context->{billing_profile} = $actual_profile;

    if ((scalar @free_cash_balances) > 0) {
        _info($context,"($context->{rownum}) " . (scalar @free_cash_balances) . ' of ' . (scalar @balances) . " contract_balances of contract id $contract_id affected by free cash",1);
        $result &= 1;
    } else {
        _info($context,"($context->{rownum}) " . "no contract_balances of contract id $contract_id affected by free cash");
        $result = 0;
    }

    #if (defined $last_balance_profile and $last_balance_profile->{interval_free_cash} > 0.0) {
    #    _info($context,"($context->{rownum}) " . 'contract id ' . $contract_id . " last balance's billing profile '$last_balance_profile->{name}': free cash $last_balance_profile->{interval_free_cash}",1);
    #    $result &= 1;
    #} elsif (defined $actual_profile and $actual_profile->{interval_free_cash} > 0.0) {
    #    _info($context,"($context->{rownum}) " . 'contract id ' . $contract_id . " actual billing profile '$actual_profile->{name}': free cash $actual_profile->{interval_free_cash}",1);
    #    $result &= 1;
    #} else {
    #    _warn($context,"($context->{rownum}) " . 'contract id ' . $contract_id . ' eventually used a billing profile with free cash in the past');
    #    $result = 0;
    #}

    return $result;

}

sub _get_balance_values {
    my ($contract_create,$last_balance,$balance) = @_;
    my ($cash_balance,$cash_balance_interval, $free_time_balance, $free_time_balance_interval) = (0.0,0.0,0,0);

    my $ratio;
    if ($last_balance) {
        if ((_CARRY_OVER_MODE eq $carry_over_mode
             || (_CARRY_OVER_TIMELY_MODE eq $carry_over_mode && $last_balance->{timely_topup_count} > 0)
            ) && (!defined $notopup_expiration || $stime < $notopup_expiration)) {
            #if (!defined $last_profile) {
            #    my $bm_last = get_actual_billing_mapping(schema => $schema, contract => $contract, now => $last_balance->start); #end); !?
            #    $last_profile = $bm_last->billing_mappings->first->billing_profile;
            #}
            #my $contract_create = NGCP::Panel::Utils::DateTime::set_local_tz($contract->create_timestamp // $contract->modify_timestamp);
            $ratio = 1.0;
            if ($last_balance->{_start} <= $contract_create && $last_balance->{_end} >= $contract_create) { #$last_balance->end is never +inf here
                $ratio = _get_free_ratio($contract_create,$last_balance->{_start},$last_balance->{_end});
            }
            my $old_free_cash = $ratio * ($last_balance->{_profile}->{interval_free_cash} // _DEFAULT_PROFILE_FREE_CASH);
            $cash_balance = $last_balance->{cash_balance};
            if ($last_balance->{cash_balance_interval} < $old_free_cash) {
                $cash_balance += $last_balance->{cash_balance_interval} - $old_free_cash;
            }
            #$ratio * $last_profile->interval_free_time // _DEFAULT_PROFILE_FREE_TIME
        } else {
            $c->log->debug('discarding contract ' . $contract->id . " cash balance (mode '$carry_over_mode'" . (defined $notopup_expiration ? ', notopup expiration ' . NGCP::Panel::Utils::DateTime::to_string($notopup_expiration) : '') . ')') if $c;
        }
        $ratio = 1.0;
    } else {
        $cash_balance = (defined $initial_balance ? $initial_balance : _DEFAULT_INITIAL_BALANCE);
        $ratio = _get_free_ratio($contract_create,$balance->{_start},$balance->{_end});
    }

    my $free_cash = $ratio * ($balance->{_profile}->{interval_free_cash} // _DEFAULT_PROFILE_FREE_CASH);
    $cash_balance += $free_cash;
    $cash_balance_interval = 0.0;

    my $free_time = $ratio * ($balance->{_profile}->{interval_free_time} // _DEFAULT_PROFILE_FREE_TIME);
    $free_time_balance = $free_time;
    $free_time_balance_interval = 0;

    $c->log->debug("ratio: $ratio, free cash: $free_cash, cash balance: $cash_balance, free time: $free_time, free time balance: $free_time_balance");

    return {cash_balance => sprintf("%.4f",$cash_balance),
            initial_cash_balance => sprintf("%.4f",$cash_balance),
            cash_balance_interval => sprintf("%.4f",$cash_balance_interval),
            free_time_balance => sprintf("%.0f",$free_time_balance),
            initial_free_time_balance => sprintf("%.0f",$free_time_balance),
            free_time_balance_interval => sprintf("%.0f",$free_time_balance_interval)};

}

sub _get_free_ratio {
    my ($contract_create,$stime,$etime) = @_;
    if (!is_infinite_future($etime)) {
        my $ctime = ($contract_create->clone->truncate(to => 'day') > $stime ? $contract_create->clone->truncate(to => 'day') : $contract_create);
        my $start_of_next_interval = _add_second($etime->clone,1);
        #$c->log->debug("ratio = " . ($start_of_next_interval->epoch - $ctime->epoch) . ' / ' . ($start_of_next_interval->epoch - $stime->epoch)) if $c;
        return ($start_of_next_interval->epoch - $ctime->epoch) / ($start_of_next_interval->epoch - $stime->epoch);
    }
    return 1.0;
}

sub _add_second {

    my ($dt,$skip_leap_seconds) = @_;
    $dt->add(seconds => 1);
    while ($skip_leap_seconds and $dt->second() >= 60) {
        $dt->add(seconds => 1);
    }
    return $dt;

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
