package NGCP::BulkProcessor::Projects::Migration::Teletek::Subscribers;
use strict;

## no critic

use threads::shared qw();
use String::MkPasswd qw();
#use List::Util qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Settings qw(
    $dry
    $skip_errors




    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads

);
#$batch

#$reseller_id
#$domain_name
#$subsciber_username_prefix
#$billing_profile_id
#$contact_email_format
#$webpassword_length
#$generate_webpassword

#$reprovision_upon_password_change
#$always_update_subscriber

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
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

#use NGCP::BulkProcessor::Projects::Migration::Teletek::Preferences qw(
#    set_preference
#    clear_preferences
#);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid);
use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_subscribers

);

sub provision_subscribers {

    my $static_context = {};
    my $result = _provision_subscribers_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    #my $updated_password_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            foreach my $domain_sipusername (@$records) {
                next unless _provision_susbcriber($context,
                    NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_domain_sipusername(@$domain_sipusername));
            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            #$context->{updated_password_count} = 0;
            # below is not mandatory..
            _check_insert_tables();
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            print('non-unique contacts: ' . join("\n",keys %{$context->{nonunique_contacts}}));
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
                #$updated_password_count += $context->{updated_password_count};
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
    my ($context,$subscriber_group) = @_;

    #my $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::get_item(1);
    return 0 unless _provision_susbcriber_init_context($context,$subscriber_group);
    #return 0;

    eval {
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states(
            $context->{db},
            $context->{domain}->{id},
            $context->{prov_subscriber}->{username},
            { 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE}
        );

        if ((scalar @$existing_billing_voip_subscribers) == 0) {

            _update_contact($context);
            _update_contract($context);
            _update_subscriber($context);

            #if ($imported_subscriber->{delta} eq
            #    $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::deleted_delta) {
            #    #_info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, and no active subscriber found');
            #} else {

            #    my $existing_provisioning_voip_dbalias = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_domainid_username($context->{db},
            #        $context->{provisioning_voip_domain}->{id},$context->{cli});

            #    #if (not defined $existing_provisioning_voip_dbalias) {
            #    #    _create_contact($context);
            #    #    _create_contract($context);
            #    #    _create_subscriber($context);
            #    #    _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' successfully provisioned',1);


            #    #} else {
            #    #    _warn($context,"($context->{rownum}) " . 'existing provisioning voip_dbalias with username ' . $context->{cli} . ' found, skipping');
            #    #}
            #}
        } elsif ((scalar @$existing_billing_voip_subscribers) == 1) {
            #my $existing_billing_voip_subscriber = $existing_billing_voip_subscribers->[0];
            #if ($imported_subscriber->{delta} eq
            #    $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::deleted_delta) {

            #    #_info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, but active subscriber found');

            #    #if (_terminate_subscriber($context,$existing_billing_voip_subscriber->{id})) {
            #    #    _terminate_contract($context,$existing_billing_voip_subscriber->{contract_id});
            #    #}

            #} else {
            #    #if ($always_update_subscriber or $context->{userpassworddelta} eq
            #    #    $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::UsernamePassword::updated_delta) {

            #    #    _info($context,"($context->{rownum}) " . 'existing billing subscriber with username ' . $context->{username} . ' and updated password found (re-provisioned)');

            #    #    if ($reprovision_upon_password_change) {
            #    #        if (_terminate_subscriber($context,$existing_billing_voip_subscriber->{id})) {
            #    #            if (_terminate_contract($context,$existing_billing_voip_subscriber->{contract_id})) {
            #    #                if ($dry) {
            #    #                    _create_contact($context);
            #    #                    _create_contract($context);
            #    #                    eval {
            #    #                        _create_subscriber($context);
            #    #                    };
            #    #                    if ($@) {
            #    #                        _info($context,"($context->{rownum}) " . 'expected error ' . $@ . ' while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode',1);
            #    #                    } else {
            #    #                        if ($skip_errors) {
            #    #                            _warn($context,"($context->{rownum}) " . 'expected error while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode missing');
            #    #                        } else {
            #    #                            _error($context,"($context->{rownum}) " . 'expected error while re-provisioning subscriber ' . $context->{cli} . ' in dry-mode missing');
            #    #                        }
            #    #                    }
            #    #                } else {
            #    #                    _create_contact($context);
            #    #                    _create_contract($context);
            #    #                    _create_subscriber($context);
            #    #                    _info($context,"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' successfully re-provisioned');
            #    #                }
            #    #            }
            #    #        }
            #    #    } else {
            #    #        _update_passwords($context,$existing_billing_voip_subscriber->{uuid});
            #    #    }
            #    #} else {
            #    #    _info($context,"($context->{rownum}) " . 'existing billing subscriber with username ' . $context->{username} . ' and unchanged password found, skipping',1);
            #    #}
            #}
        } else {
            _warn($context,(scalar @$existing_billing_voip_subscribers) . ' existing billing subscribers found, skipping');
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
            _warn($context, $err);
        } else {
            _error($context, $err);
        }
    }

    return 1;

}

sub _provision_subscribers_checks {
    my ($context) = @_;

    my $result = 1;

    my $domain_billingprofilename_resellernames = [];
    eval {
        $domain_billingprofilename_resellernames = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::list_domain_billingprofilename_resellernames();
    };
    if ($@ or (scalar @$domain_billingprofilename_resellernames) == 0) {
        rowprocessingerror(threadid(),"no domains/billing profile names/reseller names",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        $context->{domain_map} = {};
        $context->{reseller_map} = {};
        foreach my $domain_billingprofilename_resellername (@$domain_billingprofilename_resellernames) {
            my $domain = $domain_billingprofilename_resellername->{domain};
            unless ($domain) {
                rowprocessingerror(threadid(),"empty domain detected",getlogger(__PACKAGE__));
                $result = 0; #even in skip-error mode..
            }
            my $billingprofilename = $domain_billingprofilename_resellername->{billing_profile_name};
            unless ($billingprofilename) {
                rowprocessingerror(threadid(),"empty billing profile name detected",getlogger(__PACKAGE__));
                $result = 0; #even in skip-error mode..
            }
            my $resellername = $domain_billingprofilename_resellername->{reseller_name};
            unless ($resellername) {
                rowprocessingerror(threadid(),"empty reseller name detected",getlogger(__PACKAGE__));
                $result = 0; #even in skip-error mode..
            }
            if (not exists $context->{reseller_map}->{$resellername}) {
                eval {
                    $context->{reseller_map}->{$resellername} = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name($resellername);
                };
                if ($@ or not $context->{reseller_map}->{$resellername}) {
                    rowprocessingerror(threadid(),"cannot find reseller $resellername",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                } else {
                    $context->{reseller_map}->{$resellername}->{billingprofile_map} = {};
                }
            }
            if (not exists $context->{domain_map}->{$domain}) {
                eval {
                    $context->{domain_map}->{$domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($domain);
                };
                if ($@ or not $context->{domain_map}->{$domain}) {
                    rowprocessingerror(threadid(),"cannot find domain $domain (billing)",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                } else {
                    eval {
                        $context->{domain_map}->{$domain}->{prov_domain} =
                            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain);
                    };
                    if ($@ or not $context->{domain_map}->{$domain}->{prov_domain}) {
                        rowprocessingerror(threadid(),"cannot find domain $domain (provisioning)",getlogger(__PACKAGE__));
                        $result = 0; #even in skip-error mode..
                    }
                }
            }
            my $domain_reseller;
            eval {
                $domain_reseller = NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers::findby_domainid_resellerid(
                    $context->{domain_map}->{$domain}->{id},
                    $context->{reseller_map}->{$resellername}->{id})->[0];
            };
            if ($@ or not $domain_reseller) {
                rowprocessingerror(threadid(),"domain $domain does not belong to reseller $resellername",getlogger(__PACKAGE__));
                $result = 0; #even in skip-error mode..
            }

            if ($context->{reseller_map}->{$resellername}->{billingprofile_map} and
                not exists $context->{reseller_map}->{$resellername}->{billingprofile_map}->{$billingprofilename}) {

                eval {
                    $context->{reseller_map}->{$resellername}->{billingprofile_map}->{$billingprofilename} =
                        NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findby_resellerid_name_handle(
                        $context->{reseller_map}->{$resellername}->{id},
                        $billingprofilename,
                        )->[0];
                };
                if ($@ or not $context->{reseller_map}->{$resellername}->{billingprofile_map}->{$billingprofilename}) {
                    rowprocessingerror(threadid(),"cannot find billing profile $billingprofilename of reseller $resellername",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                }
            }
        }
    }



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
    #eval {
    #    $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
    #};
    #if ($@ or $subscribercount == 0) {
    #    rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    #if ($batch) {
    #    my $batch_size = 0;
    #    eval {
    #        $batch_size = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta({ 'NOT IN' =>
    #                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta});
    #    };
    #    if ($@ or $batch_size == 0) {
    #        rowprocessingerror(threadid(),'please import a batch first',getlogger(__PACKAGE__));
    #        $result = 0; #even in skip-error mode..
    #    }
    #}

    eval {
        $context->{sip_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];
    };
    if ($@ or not defined $context->{sip_account_product}) {
        rowprocessingerror(threadid(),"cannot find $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE product",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    #eval {
    #    $context->{billing_domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($domain_name);
    #    if (defined $context->{billing_domain}
    #        and NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers::countby_domainid_resellerid($context->{billing_domain}->{id},$reseller_id) == 0) {
    #        undef $context->{billing_domain};
    #    }
    #};
    #if ($@ or not defined $context->{billing_domain}) {
    #    rowprocessingerror(threadid(),'cannot find billing domain',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    #eval {
    #    $context->{provisioning_voip_domain} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain_name);
    #};
    #if ($@ or not defined $context->{provisioning_voip_domain}) {
    #    rowprocessingerror(threadid(),'cannot find provisioning domain',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    #my $billing_profile = undef;
    #eval {
    #    $billing_profile = NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findby_id($billing_profile_id);
    #    if (defined $billing_profile and $billing_profile->{reseller_id} != $reseller_id) {
    #        undef $billing_profile;
    #    }
    #};
    #if ($@ or not defined $billing_profile) {
    #    rowprocessingerror(threadid(),'cannot find billing profile',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

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

    #eval {
    #    $context->{peer_auth_pass_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
    #        $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::PEER_AUTH_PASS);
    #};

    #if ($@ or not defined $context->{peer_auth_pass_attribute}) {
    #    rowprocessingerror(threadid(),'cannot find peer_auth_pass attribute',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    return $result;
}

sub _update_contact {

    my ($context) = @_;

    if ($context->{contract}->{contact_id}) {
        NGCP::BulkProcessor::Dao::Trunk::billing::contacts::update_row($context->{db},
            { @{ $context->{contract}->{contact} }, id => $context->{contract}->{contact_id}, }
        );
    } else {
        $context->{contract}->{contact}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
            $context->{contract}->{contact},
        );
        $context->{contract}->{contact_id} = $context->{contract}->{contact}->{id};
    }

    return 1;

}

sub _update_contract {

    my ($context) = @_;

    if ($context->{bill_subscriber}->{contract_id}) {
        #todo: the update case
    } else {
        #the insert case
        $context->{contract}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
            $context->{contract}
        );
        $context->{bill_subscriber}->{contract_id} = $context->{contract}->{id};

        $context->{contract}->{billing_mapping_id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::insert_row($context->{db},
            billing_profile_id => $context->{billing_profile}->{id},
            contract_id => $context->{contract}->{id},
            product_id => $context->{sip_account_product}->{id},
        );

        $context->{contract}->{contract_balance_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
            contract_id => $context->{contract}->{id},
        );
    }
    return 1;

}

sub _update_subscriber {

    my ($context) = @_;

    my $result = 1;

    if ($context->{bill_subscriber}->{id}) {
        #todo: the update case
    } else {
        #the insert case
        $context->{bill_subscriber}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::insert_row($context->{db},
            $context->{bill_subscriber},
        );

        $context->{prov_subscriber}->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::insert_row($context->{db},
            $context->{prov_subscriber},
        );

        my $voip_number = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::forupdate_cc_ac_sn_subscriberid($context->{db},
            $context->{e164}->{cc},
            $context->{e164}->{ac},
            $context->{e164}->{sn},
            $context->{bill_subscriber}->{id});

        if (defined $voip_number) {
            $context->{voip_number_id} = $voip_number->{id};
            $result &= NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::update_row($context->{db},{
                id => $context->{voip_number_id},
                reseller_id => $context->{reseller}->{id},
                subscriber_id => $context->{bill_subscriber}->{id},
            });
        } else {
            $context->{voip_number_id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
                cc => $context->{e164}->{cc},
                ac => $context->{e164}->{ac},
                sn => $context->{e164}->{sn},
                reseller_id => $context->{reseller}->{id},
                subscriber_id => $context->{bill_subscriber}->{id},
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
    }

    return $result;

}


sub _provision_susbcriber_init_context {

    my ($context,$subscriber_group) = @_;

    my $result = 1;

    my $first = $subscriber_group->[0];

    $context->{prov_subscriber} = undef;
    unless (defined $first->{sip_username} and length($first->{sip_username})) {
        _warn($context,'empty sip_username ignored');
        $result = 0;
    }

    $context->{domain} = $context->{domain_map}->{$first->{domain}};
    $context->{reseller} = $context->{reseller_map}->{$first->{reseller_name}};
    $context->{billing_profile} = $context->{reseller}->{billingprofile_map}->{$first->{billing_profile}};

    $context->{prov_subscriber}->{username} = $first->{sip_username};
    $context->{prov_subscriber}->{password} = $first->{sip_password};
    $context->{prov_subscriber}->{web_username} = $first->{web_username};
    $context->{prov_subscriber}->{web_password} = $first->{web_password};
    $context->{prov_subscriber}->{uuid} = create_uuid();
    $context->{prov_subscriber}->{domain_id} = $context->{domain}->{prov_domain}->{id};
    #$context->{subscriber_uuid} = create_uuid();
    #$context->{mailbox_pin} = sprintf("%04d", int(rand 10000));

    $context->{bill_subscriber}->{username} = $first->{sip_username};
    $context->{bill_subscriber}->{domain_id} = $context->{domain}->{id};
    $context->{bill_subscriber}->{uuid} = $context->{prov_subscriber}->{uuid};

    undef $context->{contract};

    #if ((scalar @$subscriber_group) > 1) {
    #    print $context->{sip_username} .': ' . (scalar @$subscriber_group);
    #}

    my @aliases = ();
    my %alias_dupes = ();
    #my $contract = ();
    my %contact_dupes = ();
    foreach my $subscriber (@$subscriber_group) {
        my $number = ($subscriber->{cc} // '') . ($subscriber->{ac} // '') . ($subscriber->{sn} // '');
        if (not exists $alias_dupes{$number}) {
            push(@aliases,{
                cc => $subscriber->{cc} // '',
                ac => $subscriber->{ac} // '',
                sn => $subscriber->{sn} // '',
                username => $number,
                delta => $subscriber->{delta},
                #primary = $subscriber->{range}
            });
            $alias_dupes{$number} = 1;
        } else {
            _warn($context,'duplicate alias $number ignored');
            $context->{nonunique_contacts}->{$context->{sip_username}} += 1;
            #$result = 0;
        }

        if (not exists $contact_dupes{$subscriber->{contact_hash}}) {
            if (not $context->{contract}) {
                $context->{contract} = {
                    external_id => $subscriber->{customer_id},

                    contact => {
                        reseller_id => $context->{reseller}->{id},

                        firstname => $subscriber->{first_name},
                        lastname => $subscriber->{last_name},
                        compregnum => $subscriber->{company_registration_number},
                        company => $subscriber->{company},
                        street => $subscriber->{street},
                        postcode => $subscriber->{postal_code},
                        city => $subscriber->{city_name},
                        #country => $context->{contract}->{contact}->{country},
                        phonenumber => $subscriber->{phone_number},
                        email => $subscriber->{email},
                        vatnum => $subscriber->{vat_number},
                    },
                };
                $contact_dupes{$subscriber->{contact_hash}} = 1;
            } else {
                _warn($context,'non-unique contact hash, skipped');
                $context->{nonunique_contacts}->{$context->{sip_username}} += 1;
                #$result = 0;
            }
        }
    }
    $context->{prov_subscriber}->{aliases} = sort_by_configs(\@aliases,[
        {   numeric     => 1,
            dir         => 1, #-1,
            memberchain => [ 'sn' ],
        },
    ]);
    #my $cli

    $context->{voicemail_user}->{customer_id} = $context->{prov_subscriber}->{uuid};
    $context->{voicemail_user}->{mailbox} = $context->{cli};
    $context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));

    #if ((scalar @contacts) > 1) {
    #    print "sipuser with nonunique contact hash: " . $context->{sip_username} . ': ' . (scalar @contacts) . "\n";
    #}

    #$context->{cli} = $imported_subscriber->subscribernumber();
    #$context->{e164} = {};
    #$context->{e164}->{cc} = substr($context->{cli},0,3);
    #$context->{e164}->{ac} = '';
    #$context->{e164}->{sn} = substr($context->{cli},3);

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
    #    if ($imported_subscriber->{delta} eq
    #        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {

    #    } else {
    #        $result &= 0;

    #        # for now, as username+passwords are incomplete:
    #        #$context->{username} = $context->{e164}->{sn};
    #        #$context->{password} = $context->{username};
    #        #$context->{userpassworddelta} = $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta;

    #        if ($skip_errors) {
    #            # for now, as username+passwords are incomplete:
    #            _warn($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
    #        } else {
    #            _error($context,"($context->{rownum}) " . 'no username/password for subscriber found: ' . $context->{cli});
    #        }
    #    }
    #}

    #$context->{webusername} = $context->{username};
    #if ($generate_webpassword) {
    #    $context->{webpassword} = _generate_webpassword();
    #} else {
    #    my $webpassword = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem($context->{cli},
    #        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::WEB_PASSWORD_OPTION_SET)->[0];
    #    if (defined $webpassword) {
    #        $context->{webpassword} = $webpassword->{optionsetitem};
    #    } else {
    #        $context->{webpassword} = undef;
    #    }
    #}

    #$context->{subscriber_uuid} = create_uuid();
    #$context->{mailbox_pin} = sprintf("%04d", int(rand 10000));

    #delete $context->{contact_id};
    #delete $context->{contract_id};
    #delete $context->{billing_mapping_id};
    #delete $context->{contract_balance_id};

    #delete $context->{billing_subscriber_id};
    #delete $context->{provisioning_subscriber_id};
    #delete $context->{voip_number_id};
    #delete $context->{cli_preference_id};
    #delete $context->{voip_dbalias_id};
    #delete $context->{allowed_clis_preference_id};
    #delete $context->{account_id_preference_id};
    #delete $context->{ac_preference_id};
    #delete $context->{cc_preference_id};
    #delete $context->{voicemail_user_id};

    return $result;

}

#sub _generate_webpassword {
#    return String::MkPasswd::mkpasswd(
#        -length => $webpassword_length,
#        -minnum => 1, -minlower => 1, -minupper => 1, -minspecial => 1,
#        -distribute => 1, -fatal => 1,
#    );
#}

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
    if ($context->{prov_subscriber}) {
        $message = $context->{prov_subscriber}->{username} . ': ' . $message;
    }
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    if ($context->{prov_subscriber}) {
        $message = $context->{prov_subscriber}->{username} . ': ' . $message;
    }
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($context->{prov_subscriber}) {
        $message = $context->{prov_subscriber}->{username} . ': ' . $message;
    }
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;
