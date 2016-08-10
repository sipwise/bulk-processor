package NGCP::BulkProcessor::Projects::Migration::IPGallery::Api;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors
    $batch

    $domain_name
    $reseller_id
    $subsciber_username_prefix

    $set_call_forwards_multithreading
    $set_call_forwards_numofthreads
    $cfb_priorities
    $cfb_timeouts
    $cfu_priorities
    $cfu_timeouts
    $cft_priorities
    $cft_timeouts
    $cfna_priorities
    $cfna_timeouts
    $cfnumber_exclude_pattern
    $cfnumber_trim_pattern
    $ringtimeout
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

use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();

use NGCP::BulkProcessor::RestRequests::Trunk::CallForwards qw();

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
    set_call_forwards
    set_call_forwards_batch
);

sub set_call_forwards {

    my $static_context = {};
    my $result = _set_call_forwards_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_subscriber (@$records) {
                $rownum++;
                next unless _reset_set_call_forward_context($context,$imported_subscriber,$rownum);
                _set_call_forward($context);
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
        multithreading => $set_call_forwards_multithreading,
        numofthreads => $set_call_forwards_numofthreads,
    ),$warning_count);
}

sub set_call_forwards_batch {

    my $static_context = {};
    my $result = _set_call_forwards_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $record (@$records) {
                $rownum++;
                if ($record->{delta} ne $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta) {
                    my $imported_subscriber = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::findby_subscribernumber($record->{number});
                    if (defined $imported_subscriber) {
                        next unless _reset_set_call_forward_context($context,$imported_subscriber,$rownum);
                        _set_call_forward($context);
                    } else {
                        if ($skip_errors) {
                            _warn($context,'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number});
                            next;
                        } else {
                            _error($context,'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number});
                        }
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
        multithreading => $set_call_forwards_multithreading,
        numofthreads => $set_call_forwards_numofthreads,
    ),$warning_count);
}

sub _check_insert_tables {

}

sub _invoke_api {
    my ($context,$api_code) = @_;

    eval {
        $context->{db}->db_begin();
        #rowprocessingwarn($context->{tid},'AutoCommit is on' ,getlogger(__PACKAGE__)) if $context->{db}->{drh}->{AutoCommit};

        my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states($context->{db},
            $context->{billing_domain}->{id},$context->{username},{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE});
        if ((scalar @$existing_billing_voip_subscribers) == 0) {

            if ($context->{subscriberdelta} eq
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {
                _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, and no active subscriber found',1);
            } else {
                _warn($context,"($context->{rownum}) no active subscriber found for susbcriber " . $context->{cli});
            }
        } elsif ((scalar @$existing_billing_voip_subscribers) == 1) {
            $context->{billing_voip_subscriber} = $existing_billing_voip_subscribers->[0];
            $context->{provisioning_voip_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(
                $context->{db},$context->{billing_voip_subscriber}->{uuid});
            if (defined $context->{provisioning_voip_subscriber}) {
                if ($context->{subscriberdelta} eq
                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {

                    _warn($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, but active subscriber found');

                } else {
                    if (defined $api_code and 'CODE' eq ref $api_code) {
                        &$api_code($context);
                    }
                }
            } else {
                if ($skip_errors) {
                    _warn($context,"($context->{rownum}) " . 'no provisioning subscriber found: ' . $context->{cli});
                } else {
                    _error($context,"($context->{rownum}) " . 'no provisioning subscriber found: ' . $context->{cli});
                }
            }
        } else {
            rowprocessingwarn($context->{tid},"($context->{rownum}) " . 'multiple (' . (scalar @$existing_billing_voip_subscribers) . ') existing billing subscribers with username ' . $context->{username} . ' found, skipping' ,getlogger(__PACKAGE__));
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

}

sub _set_call_forward {
    my ($context) = @_;
    _invoke_api($context,\&_set_cf_simple);
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
    my $userpasswordcount = 0;
    eval {
        $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn();
    };
    if ($@ or $userpasswordcount == 0) {
        rowprocessingerror(threadid(),'please import user passwords first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    my $subscribercount = 0;
    my $subscriber_barring_profiles = [];
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
        $subscriber_barring_profiles = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::list_barringprofiles();
    };
    if ($@ or $subscribercount == 0) {
        rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    if ($batch) {
        my $batch_size = 0;
        eval {
            $batch_size = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta({ 'NOT IN' =>
                        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta});
        };
        if ($@ or $batch_size == 0) {
            rowprocessingerror(threadid(),'please import a batch first',getlogger(__PACKAGE__));
            $result = 0; #even in skip-error mode..
        }
    }

    eval {
        $context->{billing_domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($domain_name);
        if (defined $context->{billing_domain}
            and NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers::countby_domainid_resellerid($context->{billing_domain}->{id},$reseller_id) == 0) {
            undef $context->{billing_domain};
        }
    };
    if ($@ or not defined $context->{billing_domain}) {
        rowprocessingerror(threadid(),'cannot find billing domain',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;

}

sub _set_call_forwards_checks {
    my ($context) = @_;

    my $result = _checks($context);

    my $optioncount = 0;
    eval {
        $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option();
    };
    if ($@ or $optioncount == 0) {
        rowprocessingerror(threadid(),'please import subscriber features first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _set_cf_simple {

    my ($context) = @_;

    my $result = 0;
    my $cf_path = NGCP::BulkProcessor::RestRequests::Trunk::CallForwards::get_item_path($context->{billing_voip_subscriber}->{id});
    eval {
        my $callforwards;
        if ($dry) {
            $callforwards = NGCP::BulkProcessor::RestRequests::Trunk::CallForwards::get_item($context->{billing_voip_subscriber}->{id});
        } else {
            $callforwards = NGCP::BulkProcessor::RestRequests::Trunk::CallForwards::set_item(
                $context->{billing_voip_subscriber}->{id},$context->{call_forwards});
        }
        $result = (defined $callforwards ? 1 : 0);
    };
    if ($@ or not $result) {
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'set') . ' call forwards ' . $cf_path);
        } else {
            _error($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'set') . ' call forwards ' . $cf_path);
        }
    } else {
        _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': call forwards ' . $cf_path . ($dry ? ' fetched' : ' set'));
    }
    return $result;

}

sub _reset_context {

    my ($context,$imported_subscriber,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    $context->{cli} = $imported_subscriber->subscribernumber();
    $context->{e164} = {};
    $context->{e164}->{cc} = substr($context->{cli},0,3);
    $context->{e164}->{ac} = '';
    $context->{e164}->{sn} = substr($context->{cli},3);

    $context->{subscriberdelta} = $imported_subscriber->{delta};

    my $userpassword = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::findby_fqdn($context->{cli});
    if (defined $userpassword) {
        $context->{username} = (defined $subsciber_username_prefix ? $subsciber_username_prefix : '') . $userpassword->{username};
        $context->{password} = $userpassword->{password};
        $context->{userpassworddelta} = $userpassword->{delta};
    } else {
        # once full username+passwords is available:
        delete $context->{username};
        delete $context->{password};
        delete $context->{userpassworddelta};
        if ($context->{subscriberdelta} eq
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {

        } else {
            $result &= 0;

            # for now, as username+passwords are incomplete:
            #$context->{username} = $context->{e164}->{sn};
            #$context->{password} = $context->{username};
            #$context->{userpassworddelta} = $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta;

            if ($skip_errors) {
                # for now, as username+passwords are incomplete:
                _warn($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
            } else {
                _error($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
            }
        }
    }

    delete $context->{billing_voip_subscriber};
    delete $context->{provisioning_voip_subscriber};

    return $result;

}

sub _reset_set_call_forward_context {

    my ($context,$imported_subscriber,$rownum) = @_;

    my $result = _reset_context($context,$imported_subscriber,$rownum);

    my $call_forwards = {};
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_subscribernumber_option_optionsetitem(
            $context->{cli}, { 'IN' => [
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ON_BUSY_OPTION_SET,
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ALL_CALLS_OPTION_SET,
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ON_NO_ANSWER_OPTION_SET,
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_UNAVAILABLE_OPTION_SET,
            ]}) > 0) {

        $call_forwards->{cfb} = _prepare_callforward($context,$cfb_priorities,$cfb_timeouts,
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem(
                $context->{cli},
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ON_BUSY_OPTION_SET,
        ));

        $call_forwards->{cfu} = _prepare_callforward($context,$cfu_priorities,$cfu_timeouts,
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem(
                $context->{cli},
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ALL_CALLS_OPTION_SET,
        ));

        $call_forwards->{cft} = _prepare_callforward($context,$cft_priorities,$cft_timeouts,
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem(
                $context->{cli},
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_ON_NO_ANSWER_OPTION_SET,
        ));
        $call_forwards->{cft}->{ringtimeout} = $ringtimeout if defined $call_forwards->{cft};

        $call_forwards->{cfna} = _prepare_callforward($context,$cfna_priorities,$cfna_timeouts,
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem(
                $context->{cli},
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::FORWARD_UNAVAILABLE_OPTION_SET,
        ));
    } else {
        _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' never had call forwards, skipping',1);
        $call_forwards->{cfb} = undef;
        $call_forwards->{cfu} = undef;
        $call_forwards->{cft} = undef;
        $call_forwards->{cfna} = undef;
        $result = 0;
    }
    $context->{call_forwards} = $call_forwards;

    return $result;

}

sub _prepare_callforward {

    my ($context,$priorities,$timeouts,$cf_option_set_items) = @_;
    my @destinations = ();
    my $i = 0;
    foreach my $cf_option_set_item (@$cf_option_set_items) {
        if (defined $cf_option_set_item and $cf_option_set_item->{delta} ne
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::deleted_delta) {
            if (defined $cfnumber_exclude_pattern and $cf_option_set_item->{optionsetitem} =~ $cfnumber_exclude_pattern) {
                _warn($context,"($context->{rownum}) " . $cf_option_set_item->{option} . " '" . $cf_option_set_item->{optionsetitem} . "' of subscriber " . $context->{cli} . ': exclude pattern match');
            } else {
                my $destination = $cf_option_set_item->{optionsetitem};
                if (defined $cfnumber_trim_pattern) {
                    $destination =~ s/$cfnumber_trim_pattern//;
                    if ($cf_option_set_item->{optionsetitem} ne $destination) {
                        _info($context,"($context->{rownum}) " . $cf_option_set_item->{option} . " '" . $cf_option_set_item->{optionsetitem} . "' of subscriber " . $context->{cli} . ": trim pattern match, changed to to '$destination'");
                    }
                }
                push(@destinations, {
                    destination => $destination,
                    priority => (defined $priorities->[$i] ? $priorities->[$i] : $priorities->[-1]),
                    timeout => (defined $timeouts->[$i] ? $timeouts->[$i] : $timeouts->[-1]),
                });
                $i++;
            }
        }
    }
    if ((scalar @destinations) > 0) {
        return { destinations => \@destinations , times => [], };
    } else {
        return undef;
    }

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
