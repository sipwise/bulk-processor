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

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();



use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    fix_contract_balance_gaps
);

sub fix_contract_balance_gaps {

    my $static_context = {};
    my $result = _fix_contract_balance_gaps_checks($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::billing::contracts::process_records(
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


sub _check_insert_tables {

    #NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::check_table();

}

sub _fix_contract_balance_gaps {
    my ($context) = @_;

    eval {
        $context->{db}->db_begin();
        my $last_balance = undef;
        foreach my $contract_balance (sort NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::sort_by_end @{$context->{contract_balances}}) {
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
                        $gap_start . ' - ' . $gap_end);                        _error($context,"($context->{rownum}) " . 'no provisioning subscriber found: ' . $context->{cli});
                    }
                } elsif ($date_comparison < 0) {
                    _info($context,"($context->{rownum}) " . 'contract balances gap for contract id ' . $context->{contract}->{id} . ' detected: '.
                        $gap_start . ' - ' . $gap_end);
                    my $billing_mapping = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::findby_contractid_ts($context->{db},$context->{contract}->{id},$gap_start)->[0];
                    if (defined $billing_mapping) {
                    
                    } else {
                        if ($skip_errors) {
                            _warn($context,"($context->{rownum}) " . 'no billing mapping for contract id ' . $context->{contract}->{id} . ', t = ' . $gap_start . ' found ');
                        } else {
                            _error($context,"($context->{rownum}) " . 'no billing mapping for contract id ' . $context->{contract}->{id} . ', t = ' . $gap_start . ' found ');
                        }
                    }
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

sub _fix_contract_balance_gaps_checks {
    my ($context) = @_;

    my $result = _checks($context);

    return $result;
}

sub _reset_fix_contract_balance_gaps_context {

    my ($context,$contract,$rownum) = @_;

    my $result = _reset_context($context,$contract,$rownum);

    $context->{contract_balances} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::findby_contractid($context->{db},$context->{contract}->{id});

    #$context->{barring_profile} = $imported_subscriber->{barring_profile};
    #$context->{ncos_level} = $context->{ncos_level_map}->{$context->{barring_profile}};

    #delete $context->{adm_ncos_id_preference_id};

    return $result;

}


sub _checks  {

    my ($context) = @_;

    my $result = 1;
    #my $optioncount = 0;
    #eval {
    #    $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option();
    #};
    #if ($@ or $optioncount == 0) {
    #    rowprocessingerror(threadid(),'please import subscriber features first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}
    #my $userpasswordcount = 0;
    #eval {
    #    $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn();
    #};
    #if ($@ or $userpasswordcount == 0) {
    #    rowprocessingerror(threadid(),'please import user passwords first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}
    #my $subscribercount = 0;
    #my $subscriber_barring_profiles = [];
    #eval {
    #    $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
    #    $subscriber_barring_profiles = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::list_barringprofiles();
    #};
    #if ($@ or $subscribercount == 0) {
    #    rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    return $result;

}

sub _reset_context {

    my ($context,$contract,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{contract} = $contract;

    #$context->{cli} = $imported_subscriber->subscribernumber();
    #$context->{e164} = {};
    #$context->{e164}->{cc} = substr($context->{cli},0,3);
    #$context->{e164}->{ac} = '';
    #$context->{e164}->{sn} = substr($context->{cli},3);

    #$context->{subscriberdelta} = $imported_subscriber->{delta};

    #my $userpassword = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::findby_fqdn($context->{cli});
    #if (defined $userpassword) {
    #    $context->{username} = (defined $subsciber_username_prefix ? $subsciber_username_prefix : '') . $userpassword->{username};
    #    $context->{password} = $userpassword->{password};
    #    $context->{userpassworddelta} = $userpassword->{delta};
    #} else {
    #    # once full username+passwords is available:
    #    delete $context->{username};
    #    delete $context->{password};
    #    delete $context->{userpassworddelta};
    #    if ($context->{subscriberdelta} eq
    #        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {
    #
    #    } else {
    #        $result &= 0;
    #
    #        # for now, as username+passwords are incomplete:
    #        #$context->{username} = $context->{e164}->{sn};
    #        #$context->{password} = $context->{username};
    #        #$context->{userpassworddelta} = $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta;
    #
    #        if ($skip_errors) {
    #            # for now, as username+passwords are incomplete:
    #            _warn($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
    #        } else {
    #            _error($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
    #        }
    #    }
    #}
    #
    #delete $context->{billing_voip_subscriber};
    #delete $context->{provisioning_voip_subscriber};

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
