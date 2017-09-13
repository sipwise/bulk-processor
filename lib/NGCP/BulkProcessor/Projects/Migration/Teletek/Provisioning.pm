package NGCP::BulkProcessor::Projects::Migration::Teletek::Provisioning;
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

    $reseller_mapping

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
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli qw();

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

use NGCP::BulkProcessor::Projects::Migration::Teletek::Preferences qw(
    set_subscriber_preference
    clear_subscriber_vpreferences
    delete_subscriber_preference
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp);
use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_subscribers

);

sub provision_subscribers {

    my $static_context = { now => timestamp() };
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
            _warn($context,'non-unique contacts: ' . join("\n",keys %{$context->{nonunique_contacts}}))
                if (scalar keys %{$context->{nonunique_contacts}}) > 0;
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

    return 0 unless _provision_susbcriber_init_context($context,$subscriber_group);

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
            _create_aliases($context);
            #todo: additional prefs, AllowedIPs, NCOS, Callforwards. still thinking wether to integrate it
            #in this main provisioning loop, or align it in separate run-modes, according to the files given.

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
            my $resellername = _apply_reseller_mapping($domain_billingprofilename_resellername->{reseller_name});
            unless ($resellername) {
                rowprocessingerror(threadid(),"empty reseller name detected",getlogger(__PACKAGE__));
                $result = 0; #even in skip-error mode..
            }
            if (not exists $context->{reseller_map}->{$resellername}) {
                eval {
                    $context->{reseller_map}->{$resellername} = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name($resellername);
                };
                if ($@ or not $context->{reseller_map}->{$resellername}) {
                    rowprocessingerror(threadid(),"cannot find reseller '$resellername'",getlogger(__PACKAGE__));
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
                    rowprocessingerror(threadid(),"cannot find domain '$domain' (billing)",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                } else {
                    eval {
                        $context->{domain_map}->{$domain}->{prov_domain} =
                            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain);
                    };
                    if ($@ or not $context->{domain_map}->{$domain}->{prov_domain}) {
                        rowprocessingerror(threadid(),"cannot find domain '$domain' (provisioning)",getlogger(__PACKAGE__));
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
                    rowprocessingerror(threadid(),"cannot find billing profile '$billingprofilename' of reseller '$resellername'",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                }
            }
        }
    }

    eval {
        $context->{sip_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];
    };
    if ($@ or not defined $context->{sip_account_product}) {
        rowprocessingerror(threadid(),"cannot find $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE product",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    $context->{attributes} = {};

    eval {
        $context->{attributes}->{allowed_clis} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_CLIS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{allowed_clis}) {
        rowprocessingerror(threadid(),'cannot find allowed_clis attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{attributes}->{cli} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CLI_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cli}) {
        rowprocessingerror(threadid(),'cannot find cli attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{attributes}->{ac} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::AC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{ac}) {
        rowprocessingerror(threadid(),'cannot find ac attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{attributes}->{cc} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cc}) {
        rowprocessingerror(threadid(),'cannot find cc attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{attributes}->{account_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ACCOUNT_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{account_id}) {
        rowprocessingerror(threadid(),'cannot find account_id attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{attributes}->{concurrent_max_per_account} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CONCURRENT_MAX_PER_ACCOUNT);
    };
    if ($@ or not defined $context->{attributes}->{concurrent_max_per_account}) {
        rowprocessingerror(threadid(),'cannot find concurrent_max_per_account attribute',getlogger(__PACKAGE__));
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
        #NGCP::BulkProcessor::Dao::Trunk::billing::contacts::update_row($context->{db},
        #    { @{ $context->{contract}->{contact} }, id => $context->{contract}->{contact_id}, }
        #);
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
        $context->{prov_subscriber}->{account_id} = $context->{contract}->{id};

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

        my $number = $context->{numbers}->{primary};
        $context->{voip_numbers}->{primary} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::forupdate_cc_ac_sn_subscriberid($context->{db},
            $number->{cc},
            $number->{ac},
            $number->{sn},
            $context->{bill_subscriber}->{id});

        if (defined $context->{voip_numbers}->{primary}) {
            NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::update_row($context->{db},{
                id => $context->{voip_numbers}->{primary}->{id},
                reseller_id => $context->{reseller}->{id},
                subscriber_id => $context->{bill_subscriber}->{id},
                status => 'active',
            });
        } else {
            $context->{voip_numbers}->{primary}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
                cc => $number->{cc},
                ac => $number->{ac},
                sn => $number->{sn},
                reseller_id => $context->{reseller}->{id},
                subscriber_id => $context->{bill_subscriber}->{id},
            );
        }

        $context->{preferences}->{cli} = { id => set_subscriber_preference($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{cli},
            $number->{number}), value => $number->{number} };

        NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::update_row($context->{db},{
            id => $context->{bill_subscriber}->{id},
            primary_number_id => $context->{voip_numbers}->{primary}->{id},
        });

        #primary alias
        $context->{aliases}->{primary}->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},
            domain_id => $context->{prov_subscriber}->{domain_id},
            subscriber_id => $context->{prov_subscriber}->{id},
            username => $number->{number},
        );

        my @allowed_clis = ();
        push(@allowed_clis,{ id => set_preference($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{allowed_clis},
            $number->{number}), value => $number->{number}});
        $context->{preferences}->{allowed_clis} = \@allowed_clis;

        NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::release_subscriber_numbers($context->{db},
            $context->{bill_subscriber}->{id},{ 'NOT IN' => $context->{voip_numbers}->{primary}->{id} });

        NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::delete_dbaliases($context->{db},
            $context->{prov_subscriber}->{id},{ 'NOT IN' => $number->{number} });

        clear_subscriber_preferences($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{allowed_clis},
            $number->{number});

        $context->{voicemail_user}->{id} = NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::insert_row($context->{db},
            $context->{voicemail_user},
        );

        $context->{preferences}->{account_id} = { id => set_subscriber_preference($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{account_id},
            $context->{contract}->{id}), value => $context->{contract}->{id} };

        if (length($number->{ac}) > 0) {
            $context->{preferences}->{ac} = { id => set_subscriber_preference($context,
                $context->{prov_subscriber}->{id},
                $context->{attributes}->{ac},
                $number->{ac}), value => $number->{ac} };
        }
        if (length($number->{cc}) > 0) {
            $context->{preferences}->{cc} = { id => set_subscriber_preference($context,
                $context->{prov_subscriber}->{id},
                $context->{attributes}->{cc},
                $number->{cc}), value => $number->{cc} };
        }

                xxxxx$context->{preferences}->{cli} = { id => set_subscriber_preference($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{cli},
            $number->{number}), value => $number->{number} };
    }

    return $result;

}

sub _create_aliases {

    my ($context) = @_;
    my $result = 1;

    if ((scalar @{$context->{numbers}->{other}}) > 0) {

        my @voip_number_ids = ();
        my @usernames = ();

        foreach my $number (@{$context->{numbers}->{other}}) {

            my $voip_number = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::forupdate_cc_ac_sn_subscriberid($context->{db},
                $number->{cc},
                $number->{ac},
                $number->{sn},
                $context->{bill_subscriber}->{id});

            if (defined $voip_number) {
                NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::update_row($context->{db},{
                    id => $voip_number->{id},
                    reseller_id => $context->{reseller}->{id},
                    subscriber_id => $context->{bill_subscriber}->{id},
                    status => 'active',
                });
            } else {
                $voip_number->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
                    cc => $number->{cc},
                    ac => $number->{ac},
                    sn => $number->{sn},
                    reseller_id => $context->{reseller}->{id},
                    subscriber_id => $context->{bill_subscriber}->{id},
                );
            }

            push(@{$context->{voip_numbers}->{other}}, $voip_number);
            push(@voip_number_ids, $voip_number->{id});

            my $alias;
            if ($alias = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberid_username($context->{db},
                    $context->{prov_subscriber}->{id},
                    $number->{number},
                )->[0]) {
                NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::update_row($context->{db},{
                    id => $alias->{id},
                    is_primary => '0',
                });
                $alias->{is_primary} = '0';
            } else {
                $alias->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},{
                    domain_id => $context->{prov_subscriber}->{domain_id},
                    subscriber_id => $context->{prov_subscriber}->{id},
                    is_primary => '0',
                    username => $number->{number},
                });
            }

            push(@{$context->{aliases}->{other}},$alias);
            push(@usernames,$number->{number});

            delete_subscriber_preference($context,
                $context->{prov_subscriber}->{id},
                $context->{attributes}->{allowed_clis},
                $number->{number});
            push(@{$context->{preferences}->{allowed_clis}},{ id => set_subscriber_preference($context,
                $context->{prov_subscriber}->{id},
                $context->{attributes}->{allowed_clis},
                $number->{number}), value => $number->{number}});

        }

        push(@voip_number_ids,$context->{voip_numbers}->{primary}->{id});
        push(@usernames,$context->{numbers}->{primary}->{number});

        NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::release_subscriber_numbers($context->{db},
            $context->{bill_subscriber}->{id},{ 'NOT IN' => \@voip_number_ids });

        NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::delete_dbaliases($context->{db},$context->{prov_subscriber}->{id},
            { 'NOT IN' => \@usernames });

        clear_subscriber_preferences($context,
            $context->{prov_subscriber}->{id},
            $context->{attributes}->{allowed_clis},
             \@usernames );

    }
    return $result;
}

sub _provision_susbcriber_init_context {

    my ($context,$subscriber_group) = @_;

    my $result = 1;

    my $first = $subscriber_group->[0];

    unless (defined $first->{sip_username} and length($first->{sip_username}) > 0) {
        _warn($context,'empty sip_username ignored');
        $result = 0;
    }

    $context->{domain} = $context->{domain_map}->{$first->{domain}};
    $context->{reseller} = $context->{reseller_map}->{_apply_reseller_mapping($first->{reseller_name})};
    $context->{billing_profile} = $context->{reseller}->{billingprofile_map}->{$first->{billing_profile_name}};

    $context->{prov_subscriber} = {};
    $context->{prov_subscriber}->{username} = $first->{sip_username};
    $context->{prov_subscriber}->{password} = $first->{sip_password};
    $context->{prov_subscriber}->{webusername} = $first->{web_username};
    if (not (defined $first->{web_username} and length($first->{web_username}) > 0)) {
        $context->{prov_subscriber}->{webusername} = undef;
    } else {
        my %webusername_dupes = map { $_->{sip_username} => 1; }
            @{NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_domain_webusername(
            $first->{domain},$context->{prov_subscriber}->{webusername})};
        if ((scalar keys %webusername_dupes) > 1) {
            #_warn($context,"duplicate web_username $context->{prov_subscriber}->{webusername}, using sip_username");
            $context->{prov_subscriber}->{webusername} = $first->{sip_username};
        }
    }
    $context->{prov_subscriber}->{webpassword} = $first->{web_password};
    $context->{prov_subscriber}->{uuid} = create_uuid();
    $context->{prov_subscriber}->{domain_id} = $context->{domain}->{prov_domain}->{id};

    $context->{bill_subscriber} = {};
    $context->{bill_subscriber}->{username} = $first->{sip_username};
    $context->{bill_subscriber}->{domain_id} = $context->{domain}->{id};
    $context->{bill_subscriber}->{uuid} = $context->{prov_subscriber}->{uuid};

    undef $context->{contract};

    my @numbers = ();
    my %number_dupes = ();
    my %contact_dupes = ();
    foreach my $subscriber (@$subscriber_group) {
        my $number = ($subscriber->{cc} // '') . ($subscriber->{ac} // '') . ($subscriber->{sn} // '');
        if (not exists $number_dupes{$number}) {
            push(@numbers,{
                cc => $subscriber->{cc} // '',
                ac => $subscriber->{ac} // '',
                sn => $subscriber->{sn} // '',
                number => $number,
                delta => $subscriber->{delta},
                additional => 0,
            });
            $number_dupes{$number} = 1;
        } else {
            _warn($context,'duplicate number $number (subscriber table) ignored');
        }

        if (not exists $contact_dupes{$subscriber->{contact_hash}}) {
            if (not $context->{contract}) {
                $context->{contract} = {
                    external_id => $subscriber->{customer_id},
                    create_timestamp => $context->{now},
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
            }
        }
    }

    foreach my $allowed_cli (@{NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::findby_sipusername($first->{sip_username})}) {
        my $number = ($allowed_cli->{cc} // '') . ($allowed_cli->{ac} // '') . ($allowed_cli->{sn} // '');
        if (not exists $number_dupes{$number}) {
            push(@numbers,{
                cc => $allowed_cli->{cc} // '',
                ac => $allowed_cli->{ac} // '',
                sn => $allowed_cli->{sn} // '',
                number => $number,
                delta => $allowed_cli->{delta},
                additional => 1,
            });
            $number_dupes{$number} = 1;
        } else {
            _warn($context,'duplicate number $number (allowed_cli table) ignored');
        }
    }

    $context->{numbers} = {};
    $context->{numbers}->{other} = sort_by_configs(\@numbers,[
        {   numeric     => 1,
            dir         => 1, #-1,
            memberchain => [ 'additional' ],
        },
        {   numeric     => 0,
            dir         => 1, #-1,
            memberchain => [ 'cc' ],
        },
        {   numeric     => 0,
            dir         => 1, #-1,
            memberchain => [ 'ac' ],
        },
        {   numeric     => 0,
            dir         => 1, #-1,
            memberchain => [ 'sn' ],
        },
    ]);
    $context->{numbers}->{primary} = shift(@{$context->{numbers}->{other}});

    $context->{voip_numbers} = {};
    $context->{voip_numbers}->{primary} = undef;
    $context->{voip_numbers}->{other} = [];
    $context->{aliases} = {};
    $context->{aliases}->{primary} = undef;
    $context->{aliases}->{other} = [];

    $context->{preferences} = {};

    $context->{voicemail_user} = {};
    $context->{voicemail_user}->{customer_id} = $context->{prov_subscriber}->{uuid};
    $context->{voicemail_user}->{mailbox} = $context->{numbers}->{primary}->{number};
    $context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));

    return $result;

}

#sub _generate_webpassword {
#    return String::MkPasswd::mkpasswd(
#        -length => $webpassword_length,
#        -minnum => 1, -minlower => 1, -minupper => 1, -minspecial => 1,
#        -distribute => 1, -fatal => 1,
#    );
#}

sub _apply_reseller_mapping {
    my $reseller_name = shift;
    if (defined $reseller_name and exists $reseller_mapping->{$reseller_name}) {
        return $reseller_mapping->{$reseller_name};
    }
    return $reseller_name;
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    if ($context->{prov_subscriber}) {
        $message = ($context->{prov_subscriber}->{username} ? $context->{prov_subscriber}->{username} : '<empty sip_username>') . ': ' . $message;
    }
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    if ($context->{prov_subscriber}) {
        $message = ($context->{prov_subscriber}->{username} ? $context->{prov_subscriber}->{username} : '<empty sip_username>') . ': ' . $message;
    }
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($context->{prov_subscriber}) {
        $message = ($context->{prov_subscriber}->{username} ? $context->{prov_subscriber}->{username} : '<empty sip_username>') . ': ' . $message;
    }
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;
