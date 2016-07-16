package NGCP::BulkProcessor::Projects::Migration::IPGallery::Preferences;
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
    $set_barring_profiles_multithreading
    $set_barring_profiles_numofthreads
    $barring_profiles
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

use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();

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
    set_barring_profiles
    set_barring_profiles_batch
    clear_preferences
    set_preference
);


sub set_barring_profiles {

    my $static_context = {};
    my $result = _set_barring_profiles_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_subscriber (@$records) {
                $rownum++;
                next unless _reset_set_barring_profile_context($context,$imported_subscriber,$rownum);
                _set_barring_profile($context);
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
        multithreading => $set_barring_profiles_multithreading,
        numofthreads => $set_barring_profiles_numofthreads,
    ),$warning_count);
}

sub set_barring_profiles_batch {

    my $static_context = {};
    my $result = _set_barring_profiles_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $record (@$records) {
                $rownum++;
                my $imported_subscriber = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::findby_subscribernumber($record->{number});
                if (defined $imported_subscriber) {
                    next unless _reset_set_barring_profile_context($context,$imported_subscriber,$rownum);
                    _set_barring_profile($context);
                } else {
                    if ($skip_errors) {
                        _warn($context,'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number});
                        next;
                    } else {
                        _error($context,'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number});
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
        multithreading => $set_barring_profiles_multithreading,
        numofthreads => $set_barring_profiles_numofthreads,
    ),$warning_count);
}

sub _check_insert_tables {

    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::check_table();

}

sub _set_barring_profile {
    my ($context) = @_;
    _set_subscriber_preference($context,\&_set_adm_ncos);
}

sub _set_subscriber_preference {
    my ($context,$set_code) = @_;

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
                    if (defined $set_code and 'CODE' eq ref $set_code) {
                        &$set_code($context);
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

sub _set_barring_profiles_checks {
    my ($context) = @_;

    my $result = _checks($context);

    my $subscriber_barring_profiles = [];
    eval {
        $subscriber_barring_profiles = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::list_barringprofiles();
    };
    if ($@ or (scalar @$subscriber_barring_profiles) == 0) {
        rowprocessingerror(threadid(),'subscribers have no barring profiles',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    $context->{ncos_level_map} = {};
    foreach my $barring_profile (@$subscriber_barring_profiles) {
        if (not exists $barring_profiles->{$barring_profile}) {
            rowprocessingerror(threadid(),"mapping for barring profile '" . $barring_profile . "' missing",getlogger(__PACKAGE__));
            #$result = 0; #even in skip-error mode..
        } else {
            my $ncos_level = $barring_profiles->{$barring_profile};
            if (not defined $ncos_level or length($ncos_level) == 0) {
                $context->{ncos_level_map}->{$barring_profile} = undef;
            } else {
                eval {
                    $context->{ncos_level_map}->{$barring_profile} = NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_resellerid_level(
                        $reseller_id,$ncos_level);
                };
                if ($@ or not defined $context->{ncos_level_map}->{$barring_profile}) {
                    rowprocessingerror(threadid(),"cannot find ncos level '$ncos_level'",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                }
            }
        }
    }

    eval {
        $context->{adm_ncos_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{adm_ncos_attribute}) {
        rowprocessingerror(threadid(),'cannot find adm_ncos attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _set_adm_ncos {

    my ($context) = @_;

    $context->{adm_ncos_preference_id} = set_preference($context,$context->{provisioning_voip_subscriber}->{id},
        $context->{adm_ncos_attribute},defined $context->{ncos_level} ? $context->{ncos_level}->{id} : undef);

    _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': ncos level ' .
        (defined $context->{ncos_level} ? "'" . $context->{ncos_level}->{level} . "' set" : 'cleared') .
        " for barring profile '" . $context->{barring_profile} . "'",1);

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
        $context->{username} = $userpassword->{username};
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

sub _reset_set_barring_profile_context {

    my ($context,$imported_subscriber,$rownum) = @_;

    my $result = _reset_context($context,$imported_subscriber,$rownum);

    $context->{barring_profile} = $imported_subscriber->{barring_profile};
    $context->{ncos_level} = $context->{ncos_level_map}->{$context->{barring_profile}};

    delete $context->{adm_ncos_preference_id};

    return $result;

}























sub clear_preferences {
    my ($context,$subscriber_id,$attribute,$except_value) = @_;

    return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
        $subscriber_id,$attribute->{id},defined $except_value ? { 'NOT IN' => $except_value } : undef);

}

sub set_preference {
    my ($context,$subscriber_id,$attribute,$value) = @_;

    my $old_preferences = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::findby_subscriberid_attributeid($context->{db},
            $subscriber_id,$attribute->{id});

    if ($attribute->{max_occur} == 1) {
        if (defined $value) {
            if ((scalar @$old_preferences) == 1) {
                NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::update_row($context->{db},{
                    id => $old_preferences->[0]->{id},
                    value => $value,
                });
                return $old_preferences->[0]->{id};
            } else {
                if ((scalar @$old_preferences) > 1) {
                    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
                        $subscriber_id,$attribute->{id});
                }
                return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::insert_row($context->{db},
                    attribute_id => $attribute->{id},
                    subscriber_id => $subscriber_id,
                    value => $value,
                );
            }
        } else {
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
                $subscriber_id,$attribute->{id});
            return undef;
        }
    } else {
        if (defined $value) {
            return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::insert_row($context->{db},
                attribute_id => $attribute->{id},
                subscriber_id => $subscriber_id,
                value => $value,
            );
        } else {
            return undef;
        }
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
