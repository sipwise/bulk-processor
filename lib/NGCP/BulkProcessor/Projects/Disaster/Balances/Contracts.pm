package NGCP::BulkProcessor::Projects::Disaster::Balances::Contracts;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
use DateTime qw();

use NGCP::BulkProcessor::Globals qw(
    $system_abbreviation
);

use NGCP::BulkProcessor::Projects::Disaster::Balances::Settings qw(
    $dry
    $skip_errors

    $fix_contract_balance_gaps_multithreading
    $fix_contract_balance_gaps_numofthreads

    $fix_free_cash_multithreading
    $fix_free_cash_numofthreads

    $write_topup_log
    $apply_negative_delta
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

use NGCP::BulkProcessor::Dao::mr457::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr457::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::mr457::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::mr457::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::mr457::billing::topup_log qw();
use NGCP::BulkProcessor::Dao::mr457::billing::profile_packages qw();

#use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
#use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Array qw(array_to_map);
use NGCP::BulkProcessor::Calendar qw(current_local is_infinite_future);
use NGCP::BulkProcessor::SqlConnectors::MySQLDB qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    fix_contract_balance_gaps
    fix_free_cash
);

my $create_sample_cdr = 0; 

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
    return ($result && NGCP::BulkProcessor::Dao::mr457::billing::contracts::process_free_cash_contracts(
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

        $context->{db}->set_transaction_isolation($NGCP::BulkProcessor::SqlConnectors::MySQLDB::READ_COMMITTED);
        $context->{db}->db_begin();
        my $contract_id = $context->{contract}->{id};
        my $contract = NGCP::BulkProcessor::Dao::mr457::billing::contracts::forupdate_id($context->{db},$contract_id);
        $context->{contract} = $contract;
        my $actual_profile = $context->{actual_profile};

        my @balances = sort NGCP::BulkProcessor::Dao::mr457::billing::contract_balances::sort_by_end_desc
            @{NGCP::BulkProcessor::Dao::mr457::billing::contract_balances::findby_contractid($context->{db},$contract_id)};
        my $balance = $balances[0];
        my $mapping = NGCP::BulkProcessor::Dao::mr457::billing::billing_mappings::findby_contractid_ts($context->{db},
            $contract_id,$balance->{_start})->[0];
        if ($mapping) {
            my $billing_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
            my $free_cash = $billing_profile->{interval_free_cash} // 0.0;
            my $next_free_cash = $actual_profile->{interval_free_cash} // 0.0;
            my $free_cash_carry_over = $next_free_cash;
            if ($balance->{cash_balance_interval} < $free_cash) {
                $free_cash_carry_over += $balance->{cash_balance_interval} - $free_cash;
            }
            my $delta = $next_free_cash - ($balance->{cash_balance} + $free_cash_carry_over);

            if ($delta > 0.0 or ($delta < 0.0 and $apply_negative_delta)) {
                NGCP::BulkProcessor::Dao::mr457::billing::contract_balances::update_row($context->{db},{
                    id => $balance->{id},
                    cash_balance => ($balance->{cash_balance} + $delta),
                });

                NGCP::BulkProcessor::Dao::mr457::billing::topup_log::insert_row($context->{db},{
                    timestamp => $context->{now}->epoch(),
                    type => $NGCP::BulkProcessor::Dao::mr457::billing::topup_log::CASH_TYPE, #SET_BALANCE_TYPE,
                    outcome => $NGCP::BulkProcessor::Dao::mr457::billing::topup_log::OK_OUTCOME,
                    contract_id => $contract_id,
                    amount => $delta,
                    cash_balance_before => $balance->{cash_balance},
                    cash_balance_after => ($balance->{cash_balance} + $delta),
                    package_before_id => $contract->{profile_package_id},
                    package_after_id => $contract->{profile_package_id},
                    profile_before_id => $context->{actual_profile}->{id},
                    profile_after_id => $context->{actual_profile}->{id},
                    contract_balance_before_id => $balance->{id},
                    contract_balance_after_id => $balance->{id},
                    request_token => $system_abbreviation,
                }) if $write_topup_log;

                _info($context,"($context->{rownum}) " . "contract id $contract_id cash balance is $balance->{cash_balance} cents, adding $delta cents to match free cash of $context->{actual_profile}->{interval_free_cash} cents ($context->{actual_profile}->{name}) for next interval"
                      . (is_infinite_future($balance->{_end}) ? '' : ' ' . $balance->{_end}->clone->add(seconds => 1)));

                if ($create_sample_cdr and not is_infinite_future($balance->{_end}) and _generate_cdr_init_context($context,$balance->{_end}->clone->add(seconds => 1))) {
                    NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::insert_row($context->{db},$context->{cdr});
                }

            } else {
                _info($context,"($context->{rownum}) " . "contract id $contract_id cash balance is $balance->{cash_balance} cents, SKIP adding $delta cents to match free cash of $context->{actual_profile}->{interval_free_cash} cents ($context->{actual_profile}->{name}) for next interval"
                      . (is_infinite_future($balance->{_end}) ? '' : ' ' . $balance->{_end}->clone->add(seconds => 1)));
            }

        } else {
            if ($skip_errors) {
                _warn($context,"($context->{rownum}) " . "no billing mapping at $balance->{_start} for contract id $contract_id");
            } else {
                _error($context,"($context->{rownum}) " . "no billing mapping at $balance->{_start} for contract id $contract_id");
            }
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


sub _fix_free_cash_checks {
    my ($context) = @_;

    my $result = 1;

    $context->{now} = current_local();

    my $profile_count = 0;
    eval {
        ($context->{billing_profile_map},my $ids,my $profiles) = array_to_map(NGCP::BulkProcessor::Dao::mr457::billing::billing_profiles::findall(),
            sub { return shift->{id}; }, sub { return shift; }, 'first' );
        $profile_count = (scalar keys %{$context->{billing_profile_map}});
    };
    if ($@ or $profile_count == 0) {
        _error($context,"cannot find any billing profiles");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$profile_count billing profiles cached");
    }

    my $package_count = 0;
    eval {
        ($context->{profile_package_map},my $ids,my $packages) = array_to_map(NGCP::BulkProcessor::Dao::mr457::billing::profile_packages::findall(),
            sub { return shift->{id}; }, sub { return shift; }, 'first' );
        $package_count = (scalar keys %{$context->{profile_package_map}});
    };
    if ($@) {
        _error($context,"cannot find profile packages");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$package_count profile packages cached");
    }

    if ($create_sample_cdr) {
        my $domain_count = 0;
        eval {
            ($context->{domain_map},my $ids,my $domains) = array_to_map(NGCP::BulkProcessor::Dao::Trunk::billing::domains::findall(),
                sub { return shift->{id}; }, sub { return shift; }, 'first' );
            $domain_count = (scalar keys %{$context->{domain_map}});
        };
        if ($@ or $domain_count == 0) {
            _error($context,"cannot find any domains");
            $result = 0; #even in skip-error mode..
        } else {
            _info($context,"$domain_count domains cached");
        }

        my $reseller_count = 0;
        eval {
            ($context->{reseller_map},my $ids,my $resellers) = array_to_map(NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findall(),
                sub { return shift->{id}; }, sub { return shift; }, 'first' );
            $reseller_count = (scalar keys %{$context->{reseller_map}});
        };
        if ($@ or $reseller_count == 0) {
            _error($context,"cannot find any resellers");
            $result = 0; #even in skip-error mode..
        } else {
            _info($context,"$reseller_count resellers cached");
        }

        my $active_count = 0;
        eval {
            $active_count = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(
                $NGCP::BulkProcessor::Dao::Trunk::billing::contracts::ACTIVE_STATE,
                undef
            );
            ($context->{min_id},$context->{max_id}) = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::find_minmaxid(undef,
                { 'IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::ACTIVE_STATE },
                undef
            );
        };
        if ($@ or $active_count == 0) {
            _error($context,"cannot find active subscribers");
            $result = 0; #even in skip-error mode..
        } else {
            _info($context,"$active_count active subscribers found");
        }
    }

    my $contract_free_cash_count = 0;
    eval {
        $contract_free_cash_count = NGCP::BulkProcessor::Dao::mr457::billing::contracts::countby_free_cash();
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

    my $contract = NGCP::BulkProcessor::Dao::mr457::billing::contracts::findby_id($contract_id);

    $context->{contract} = $contract;

    # the goal is to adjust the latest contract_balances.cash_balance, so the
    # next contract_balance will start with the exact free cash expected:

    my $actual_profile;
    my $mapping = NGCP::BulkProcessor::Dao::mr457::billing::billing_mappings::findby_contractid_ts($context->{db},
            $contract_id,$context->{now})->[0]; # the actual profile is considered for the next balance interval
    if ($mapping) {
        $actual_profile = $context->{billing_profile_map}->{$mapping->{billing_profile_id}};
        if (($actual_profile->{interval_free_cash} // 0.0) != 0.0) {
            my $carry_over_mode = $NGCP::BulkProcessor::Dao::mr457::billing::profile_packages::DEFAULT_CARRY_OVER_MODE;
            $carry_over_mode = $context->{profile_package_map}->{$contract->{profile_package_id}}->{carry_over_mode} if $contract->{profile_package_id};
            if ($carry_over_mode ne $NGCP::BulkProcessor::Dao::mr457::billing::profile_packages::DISCARD_MODE) {
                $result = 1;
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

    $context->{actual_profile} = $actual_profile;

    return $result;

}

sub _generate_cdr_init_context {

    my ($context,$time) = @_;

    #my $result = 1;

    #my $provider = $providers[rand @providers];

	my $source_subscriber;
    $source_subscriber = _get_subscriber($context);
    my $dest_subscriber;
    $dest_subscriber = undef;

    return 0 unless $source_subscriber;

    my $source_peering_subscriber_info;
    my $source_reseller;
    if ($source_subscriber) {
        $source_subscriber->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id($source_subscriber->{contract_id});
        $source_subscriber->{contract}->{contact} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_id($source_subscriber->{contract}->{contact_id});
        $source_subscriber->{contract}->{prov_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$source_subscriber->{uuid});
        $source_subscriber->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($source_subscriber->{contract}->{prov_subscriber}->{id},1);
        $source_subscriber->{domain} = $context->{domain_map}->{$source_subscriber->{domain_id}}->{domain};
        $source_reseller = $context->{reseller_map}->{$source_subscriber->{contract}->{contact}->{reseller_id}};
    } else {
        $source_peering_subscriber_info = _prepare_offnet_subscriber_info("source","offnet.com");
    }

    my $dest_peering_subscriber_info;
    my $dest_reseller;
    if ($dest_subscriber) {
        $dest_subscriber->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id($dest_subscriber->{contract_id});
        $dest_subscriber->{contract}->{contact} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_id($dest_subscriber->{contract}->{contact_id});
        $dest_subscriber->{contract}->{prov_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$dest_subscriber->{uuid});
        $dest_subscriber->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($dest_subscriber->{contract}->{prov_subscriber}->{id},1);
        $dest_subscriber->{domain} = $context->{domain_map}->{$dest_subscriber->{domain_id}}->{domain};
        $dest_reseller = $context->{reseller_map}->{$dest_subscriber->{contract}->{contact}->{reseller_id}};
    } else {
        $dest_peering_subscriber_info = _prepare_offnet_subscriber_info("destination","offnet.com");
    }

    my $source_ip = '192.168.0.1';
    #my $time = time();
    my $duration = 0;

	$context->{cdr} = {
		#id                                     => ,
		#update_time                            => ,
		source_user_id                          => ($source_subscriber ? $source_subscriber->{uuid} : '0'),
		source_provider_id                      => ($source_reseller ? $source_reseller->{contract_id} : '0'),
		#source_external_subscriber_id          => ,
		#source_external_contract_id            => ,
		source_account_id                       => ($source_subscriber ? $source_subscriber->{contract_id} : '0'),
		source_user                             => ($source_subscriber ? $source_subscriber->{username} : $source_peering_subscriber_info->{username}),
		source_domain                           => ($source_subscriber ? $source_subscriber->{domain} : $source_peering_subscriber_info->{domain}),
		source_cli                              => ($source_subscriber ? ($source_subscriber->{primary_alias}->{username} // $source_subscriber->{username}) : $source_peering_subscriber_info->{username}),
		#source_clir                            => '0',
		source_ip                               => $source_ip,
		#source_gpp0                            => ,
		#source_gpp1                            => ,
		#source_gpp2                            => ,
		#source_gpp3                            => ,
		#source_gpp4                            => ,
		#source_gpp5                            => ,
		#source_gpp6                            => ,
		#source_gpp7                            => ,
		#source_gpp8                            => ,
		#source_gpp9                            => ,
		destination_user_id                     => ($dest_subscriber ? $dest_subscriber->{uuid} : '0'),
		destination_provider_id                 => ($dest_reseller ? $dest_reseller->{contract_id} : '0'),
		#destination_external_subscriber_id     => ,
		#destination_external_contract_id       => ,
		destination_account_id                  => ($dest_subscriber ? $dest_subscriber->{contract_id} : '0'),
		destination_user                        => ($dest_subscriber ? $dest_subscriber->{username} : $dest_peering_subscriber_info->{username}),
		destination_domain                      => ($dest_subscriber ? $dest_subscriber->{domain} : $dest_peering_subscriber_info->{domain}),
		destination_user_dialed                 => ($dest_subscriber ? ($dest_subscriber->{primary_alias}->{username} // $dest_subscriber->{username}) : $dest_peering_subscriber_info->{username}),
		destination_user_in                     => ($dest_subscriber ? ($dest_subscriber->{primary_alias}->{username} // $dest_subscriber->{username}) : $dest_peering_subscriber_info->{username}),
		destination_domain_in                   => ($dest_subscriber ? $dest_subscriber->{domain} : $dest_peering_subscriber_info->{domain}),
		#destination_gpp0                       => ,
		#destination_gpp1                       => ,
		#destination_gpp2                       => ,
		#destination_gpp3                       => ,
		#destination_gpp4                       => ,
		#destination_gpp5                       => ,
		#destination_gpp6                       => ,
		#destination_gpp7                       => ,
		#destination_gpp8                       => ,
		#destination_gpp9                       => ,
		#peer_auth_user                         => ,
		#peer_auth_realm                        => ,
		call_type                               => 'call',
		call_status                             => 'ok',
		call_code                               => '200',
		init_time                               => $time->epoch,
		start_time                              => $time->epoch,
		duration                                => $duration,
		call_id                                 => _generate_call_id(),
		#source_carrier_cost                    => ,
		#source_reseller_cost                   => ,
		#source_customer_cost                   => ,
		#source_carrier_free_time               => ,
		#source_reseller_free_time              => ,
		#source_customer_free_time              => ,
		#source_carrier_billing_fee_id          => ,
		#source_reseller_billing_fee_id         => ,
		#source_customer_billing_fee_id         => ,
		#source_carrier_billing_zone_id         => ,
		#source_reseller_billing_zone_id        => ,
		#source_customer_billing_zone_id        => ,
		#destination_carrier_cost               => ,
		#destination_reseller_cost              => ,
		#destination_customer_cost              => ,
		#destination_carrier_free_time          => ,
		#destination_reseller_free_time         => ,
		#destination_customer_free_time         => ,
		#destination_carrier_billing_fee_id     => ,
		#destination_reseller_billing_fee_id    => ,
		#destination_customer_billing_fee_id    => ,
		#destination_carrier_billing_zone_id    => ,
		#destination_reseller_billing_zone_id   => ,
		#destination_customer_billing_zone_id   => ,
		#frag_carrier_onpeak                    => ,
		#frag_reseller_onpeak                   => ,
		#frag_customer_onpeak                   => ,
		#is_fragmented                          => ,
		#split                                  => ,
		#rated_at                               => ,
		#rating_status                          => 'unrated',
		#exported_at                            => ,
		#export_status                          => ,
	};

    return 1;

}

sub _get_subscriber {
    my ($context,$excluding_id) = @_;

    return NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_contractid_states(
        $context->{db},
        $context->{contract}->{id},
        { 'IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::ACTIVE_STATE },
    )->[0];
}

sub _prepare_offnet_subscriber_info {
	my ($username_primary_number,$domain) = @_;
	return { username => $username_primary_number, domain => $domain };
}

sub _generate_call_id {
	return '*TEST*'._random_string(26,'a'..'z','A'..'Z',0..9,'-','.');
}

sub _random_string {
	my ($length,@chars) = @_;
	return join('',@chars[ map{ rand @chars } 1 .. $length ]);
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
