package NGCP::BulkProcessor::Projects::Migration::IPGallery::Provisioning;
use strict;

## no critic

use threads::shared qw();
use String::MkPasswd qw();
#use List::Util qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors
    $batch

    $reseller_id
    $domain_name
    $billing_profile_id
    $contact_email_format
    $webpassword_length
    $generate_webpassword

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
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

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Subscribers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Preferences qw(
    set_preference
    clear_preferences
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_subscribers
    provision_subscribers_batch
);

sub provision_subscribers {

    my $static_context = {};
    my $result = _provision_subscribers_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_subscriber (@$records) {
                $rownum++;
                next unless _provision_susbcriber($context,$imported_subscriber,$rownum);
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
        multithreading => $provision_subscriber_multithreading,
        numofthreads => $provision_subscriber_numofthreads,
    ),$warning_count);
}

sub provision_subscribers_batch {

    my $static_context = {};
    my $result = _provision_subscribers_checks($static_context);

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
                    next unless _provision_susbcriber($context,$imported_subscriber,$rownum);
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
        multithreading => $provision_subscriber_multithreading,
        numofthreads => $provision_subscriber_numofthreads,
    ),$warning_count);
}


sub _check_insert_tables {

    NGCP::BulkProcessor::Dao::Trunk::billing::contacts::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::contracts::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::check_table();
    NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::check_table();

}

sub _provision_susbcriber {
    my ($context,$imported_subscriber,$rownum) = @_;

    return 0 unless _reset_context($context,$imported_subscriber,$rownum);

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

sub _provision_subscribers_checks {
    my ($context) = @_;

    my $result = 1;
    my $optioncount = 0;
    eval {
        $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option();
    };
    if ($@ or $optioncount == 0) {
        rowprocessingerror(threadid(),'please import subscriber features first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    my $userpasswordcount = 0;
    eval {
        $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn();
    };
    if ($@ or $userpasswordcount == 0) {
        rowprocessingerror(threadid(),'please import user passwords first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
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
        $context->{sip_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];
    };
    if ($@ or not defined $context->{sip_account_product}) {
        rowprocessingerror(threadid(),'cannot find sip account product',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
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

    eval {
        $context->{provisioning_voip_domain} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain_name);
    };
    if ($@ or not defined $context->{provisioning_voip_domain}) {
        rowprocessingerror(threadid(),'cannot find provisioning domain',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    my $billing_profile = undef;
    eval {
        $billing_profile = NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findby_id($billing_profile_id);
        if (defined $billing_profile and $billing_profile->{reseller_id} != $reseller_id) {
            undef $billing_profile;
        }
    };
    if ($@ or not defined $billing_profile) {
        rowprocessingerror(threadid(),'cannot find billing profile',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{allowed_clis_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_CLIS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{allowed_clis_attribute}) {
        rowprocessingerror(threadid(),'cannot find allowed_clis attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{cli_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CLI_ATTRIBUTE);
    };
    if ($@ or not defined $context->{cli_attribute}) {
        rowprocessingerror(threadid(),'cannot find cli attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{ac_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::AC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{ac_attribute}) {
        rowprocessingerror(threadid(),'cannot find ac attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{cc_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{cc_attribute}) {
        rowprocessingerror(threadid(),'cannot find cc attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{account_id_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ACCOUNT_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{account_id_attribute}) {
        rowprocessingerror(threadid(),'cannot find account_id attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _create_contact {

    my ($context) = @_;

    $context->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
        reseller_id => $reseller_id,
        email => sprintf($contact_email_format,$context->{username}), #$context->{cli}
    );
    #my $contact_id = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($db,{
    #    reseller_id => $reseller_id,
    #    email => sprintf($contact_email_format,$cli),
    #});

    return 1;

}

sub _create_contract {

    my ($context) = @_;

    $context->{contract_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
        contact_id => $context->{contact_id},
    );

    $context->{billing_mapping_id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::insert_row($context->{db},
        billing_profile_id => $billing_profile_id,
        contract_id => $context->{contract_id},
        product_id => $context->{sip_account_product}->{id},
    );

    $context->{contract_balance_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
        contract_id => $context->{contract_id},
    );

    return 1;

}

sub _create_subscriber {

    my ($context) = @_;

    my $result = 1;

    $context->{billing_subscriber_id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::insert_row($context->{db},
        contract_id => $context->{contract_id},
        domain_id => $context->{billing_domain}->{id},
        username => $context->{username},
        uuid => $context->{subscriber_uuid},
    );

    $context->{provisioning_subscriber_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::insert_row($context->{db},
        account_id => $context->{contract_id},
        domain_id => $context->{provisioning_voip_domain}->{id},
        password => $context->{password},
        username => $context->{username},
        uuid => $context->{subscriber_uuid},
        webpassword => $context->{webpassword},
        webusername => $context->{webusername},
    );

    my $voip_number = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::forupdate_cc_ac_sn_subscriberid($context->{db},
        $context->{e164}->{cc},$context->{e164}->{ac},$context->{e164}->{sn},$context->{billing_subscriber_id});

    if (defined $voip_number) {
        $context->{voip_number_id} = $voip_number->{id};
        $result &= NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::update_row($context->{db},{
            id => $context->{voip_number_id},
            reseller_id => $reseller_id,
            subscriber_id => $context->{billing_subscriber_id},
        });
    } else {
        $context->{voip_number_id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
            cc => $context->{e164}->{cc},
            ac => $context->{e164}->{ac},
            sn => $context->{e164}->{sn},
            reseller_id => $reseller_id,
            subscriber_id => $context->{billing_subscriber_id},
        );
    }

    $context->{cli_preference_id} = set_preference($context,$context->{provisioning_subscriber_id},$context->{cli_attribute},$context->{cli});

    $result &= NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::update_row($context->{db},{
        id => $context->{billing_subscriber_id},
        primary_number_id => $context->{voip_number_id},
    });

    $context->{voip_dbalias_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},
        domain_id => $context->{provisioning_voip_domain}->{id},
        subscriber_id => $context->{provisioning_subscriber_id},
        username => $context->{cli},
    );

    $context->{allowed_clis_preference_id} = set_preference($context,$context->{provisioning_subscriber_id},$context->{allowed_clis_attribute},$context->{cli});

    $result &= NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::release_subscriber_numbers($context->{db},
        $context->{billing_subscriber_id},{ 'NOT IN' => $context->{voip_number_id} });

    $result &= NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::delete_dbaliases($context->{db},
        $context->{provisioning_voip_domain}->{id},{ 'NOT IN' => $context->{cli} });

    clear_preferences($context,$context->{provisioning_subscriber_id},$context->{allowed_clis_attribute},$context->{cli});

    #voicemail
    $context->{voicemail_user_id} = NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::insert_row($context->{db},
        customer_id => $context->{subscriber_uuid},
        mailbox => $context->{cli},
        password => $context->{mailbox_pin},
    );

    $context->{account_id_preference_id} = set_preference($context,$context->{provisioning_subscriber_id},$context->{account_id_attribute},$context->{contract_id});
    if (defined $context->{e164}->{ac} and length($context->{e164}->{ac}) > 0) {
        $context->{ac_preference_id} = set_preference($context,$context->{provisioning_subscriber_id},$context->{ac_attribute},$context->{e164}->{ac});
    }
    if (defined $context->{e164}->{cc} and length($context->{e164}->{cc}) > 0) {
        $context->{cc_preference_id} = set_preference($context,$context->{provisioning_subscriber_id},$context->{cc_attribute},$context->{e164}->{cc});
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
        if ($imported_subscriber->{delta} eq
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

    $context->{webusername} = $context->{username};
    if ($generate_webpassword) {
        $context->{webpassword} = _generate_webpassword();
    } else {
        my $webpassword = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem($context->{cli},
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::WEB_PASSWORD_OPTION_SET)->[0];
        if (defined $webpassword) {
            $context->{webpassword} = $webpassword->{optionsetitem};
        } else {
            $context->{webpassword} = undef;
        }
    }

    $context->{subscriber_uuid} = create_uuid();
    $context->{mailbox_pin} = sprintf("%04d", int(rand 10000));

    delete $context->{contact_id};
    delete $context->{contract_id};
    delete $context->{billing_mapping_id};
    delete $context->{contract_balance_id};

    delete $context->{billing_subscriber_id};
    delete $context->{provisioning_subscriber_id};
    delete $context->{voip_number_id};
    delete $context->{cli_preference_id};
    delete $context->{voip_dbalias_id};
    delete $context->{allowed_clis_preference_id};
    delete $context->{account_id_preference_id};
    delete $context->{ac_preference_id};
    delete $context->{cc_preference_id};
    delete $context->{voicemail_user_id};

    return $result;

}

sub _generate_webpassword {
    return String::MkPasswd::mkpasswd(
        -length => $webpassword_length,
        -minnum => 1, -minlower => 1, -minupper => 1, -minspecial => 1,
        -distribute => 1, -fatal => 1,
    );
}

sub _terminate_subscriber {
    my ($context,$billing_subscriber_id) = @_;

    my $result = 0;
    my $subscriber_path = NGCP::BulkProcessor::RestRequests::Trunk::Subscribers::get_item_path($billing_subscriber_id);
    eval {
        if ($dry) {
            my $subscriber = NGCP::BulkProcessor::RestRequests::Trunk::Subscribers::get_item($billing_subscriber_id);
            $result = (defined $subscriber ? 1 : 0);
        } else {
            $result = NGCP::BulkProcessor::RestRequests::Trunk::Subscribers::delete_item($billing_subscriber_id);
        }
    };
    if ($@ or not $result) {
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'terminate') . ' old subscriber ' . $subscriber_path);
        } else {
            _error($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'terminate') . ' old subscriber ' . $subscriber_path);
        }
    } else {
        _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': old subscriber ' . $subscriber_path . ($dry ? ' fetched' : ' terminated'));
    }
    return $result;

}

sub _terminate_contract {
    my ($context,$contract_id) = @_;

    my $result = 0;
    my $contract_path = NGCP::BulkProcessor::RestRequests::Trunk::Customers::get_item_path($contract_id);
    eval {
        my $customer;
        if ($dry) {
            $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::get_item($contract_id);
        } else {
            $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::update_item($contract_id,{
                status => $NGCP::BulkProcessor::RestRequests::Trunk::Customers::TERMINATED_STATE,
            });
        }
        $result = (defined $customer ? 1 : 0);
    };
    if ($@ or not $result) {
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'terminate') . ' old contract ' . $contract_path);
        } else {
            _error($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': could not ' . ($dry ? 'fetch' : 'terminate') . ' old contract ' . $contract_path);
        }
    } else {
        _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ': old contract ' . $contract_path . ($dry ? ' fetched' : ' terminated'));
    }
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
