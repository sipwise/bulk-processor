package NGCP::BulkProcessor::Projects::Migration::UPCAT::Provisioning;
use strict;

## no critic

use threads::shared qw();
use String::MkPasswd qw();
#use List::Util qw();

use JSON -support_by_pp, -no_export;
use Tie::IxHash;

use NGCP::BulkProcessor::Projects::Migration::UPCAT::Settings qw(
    $dry
    $skip_errors
    $report_filename

    $provision_mta_subscriber_multithreading
    $provision_mta_subscriber_numofthreads
    $mta_webpassword_length
    $mta_webusername_length
    $mta_sippassword_length

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
    fileerror
);

use NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();
#use NGCP::BulkProcessor::Dao::Trunk::kamailio::location qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Subscribers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

use NGCP::BulkProcessor::Projects::Migration::UPCAT::Preferences qw(
    set_subscriber_preference
    get_subscriber_preference
    clear_subscriber_preferences
    delete_subscriber_preference
);
#set_allowed_ips_preferences
#cleanup_aig_sequence_ids

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::UPCAT::ProjectConnectorPool qw(
    destroy_all_dbs
    ping_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp stringtobool trim); #check_ipnet
use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
use NGCP::BulkProcessor::RandomString qw(createtmpstring);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_mta_subscribers

);

my $split_ipnets_pattern =  join('|',(
    quotemeta(','),
    quotemeta(';'),
    #quotemeta('/')
));

my $db_lock :shared = undef;
my $file_lock :shared = undef;

my $default_barring = 'default';

sub provision_mta_subscribers {

    my $static_context = { now => timestamp(), _rowcount => undef };
    my $result = _provision_mta_subscribers_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    my %nonunique_contacts :shared = ();
    return ($result && NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            $context->{_rowcount} = $row_offset;
            my @report_data = ();
            foreach my $domain_sipusername (@$records) {
                $context->{_rowcount} += 1;
                next unless _provision_mta_susbcriber($context,
                    NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::findby_domain_sipusername(@$domain_sipusername));
                push(@report_data,_get_report_obj($context));
            }
            #cleanup_aig_sequence_ids($context);
            if (defined $report_filename) {
                lock $file_lock;
                open(my $fh, '>>', $report_filename) or fileerror('cannot open file ' . $report_filename . ': ' . $!,getlogger(__PACKAGE__));
                binmode($fh);
                print $fh JSON::to_json(\@report_data,{ allow_nonref => 1, allow_blessed => 1, convert_blessed => 1, pretty => 1, });
                close $fh;
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            $context->{nonunique_contacts} = {};

        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
            {
                lock %nonunique_contacts;
                foreach my $sip_username (keys %{$context->{nonunique_contacts}}) {
                    $nonunique_contacts{$sip_username} = $context->{nonunique_contacts}->{$sip_username};
                }
            }
        },
        load_recursive => 0,
        multithreading => $provision_mta_subscriber_multithreading,
        numofthreads => $provision_mta_subscriber_numofthreads,
    ),$warning_count,\%nonunique_contacts);

}

sub _get_report_obj {
    my ($context) = @_;
    my %dump = ();
    tie(%dump, 'Tie::IxHash');
    foreach my $key (sort keys %$context) {
        $dump{$key} = $context->{$key} if 'CODE' ne ref $context->{$key};
    }
    foreach my $key (qw/
        sip_account_product
        reseller
        billing_profile
        reseller_map
        domain_map
        domain
        now
        error_count
        warning_count
        attributes
        ncos_level_map
        ncos_level
        nonunique_contacts
        tid
        db
        blocksize
        errorstates
        queue
        readertid
        /) {
        delete $dump{$key};
    }
    return \%dump;
}

sub _provision_mta_susbcriber {
    my ($context,$subscriber_group) = @_;

    return 0 unless _provision_mta_susbcriber_init_context($context,$subscriber_group);

    eval {
        lock $db_lock;
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states(
            $context->{db},
            $context->{domain}->{id},
            $context->{prov_subscriber}->{username},
            { 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE}
        );

        if ((scalar @$existing_billing_voip_subscribers) == 0) {

            _update_mta_contact($context);
            _update_mta_contract($context);
            #{
            #    lock $db_lock; #concurrent writes to voip_numbers causes deadlocks
                _update_subscriber($context);
                _create_aliases($context);
            #}
            _update_mta_preferences($context);
            #_set_registrations($context);
            #_set_callforwards($context);
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

sub _provision_mta_subscribers_checks {
    my ($context) = @_;

    my $result = 1;

    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::countby_ccacsn();
    };
    if ($@ or $subscribercount == 0) {
        rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"$subscribercount subscriber found",getlogger(__PACKAGE__));
    }

    my $domain_billingprofilename_resellernames = [];
    eval {
        $domain_billingprofilename_resellernames = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::list_domain_billingprofilename_resellernames();
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
                    processing_info(threadid(),"reseller '$resellername' found",getlogger(__PACKAGE__));
                }
            }
            if (not exists $context->{domain_map}->{$domain}) {
                eval {
                    $context->{domain_map}->{$domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($domain);
                };
                if ($@ or not $context->{domain_map}->{$domain}) {
                    rowprocessingerror(threadid(),"cannot find billing domain '$domain'",getlogger(__PACKAGE__));
                    $result = 0; #even in skip-error mode..
                } else {
                    processing_info(threadid(),"billing domain '$domain' found",getlogger(__PACKAGE__));
                    eval {
                        $context->{domain_map}->{$domain}->{prov_domain} =
                            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain);
                    };
                    if ($@ or not $context->{domain_map}->{$domain}->{prov_domain}) {
                        rowprocessingerror(threadid(),"cannot find provisioning domain '$domain'",getlogger(__PACKAGE__));
                        $result = 0; #even in skip-error mode..
                    } else {
                        processing_info(threadid(),"provisioning domain '$domain' found",getlogger(__PACKAGE__));
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
            } else {
                processing_info(threadid(),"domain $domain belongs to reseller $resellername",getlogger(__PACKAGE__));
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
                } else {
                    processing_info(threadid(),"billing profile '$billingprofilename' of reseller '$resellername' found",getlogger(__PACKAGE__));
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
    } else {
        processing_info(threadid(),"$NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE product found",getlogger(__PACKAGE__));
    }

    $context->{attributes} = {};

    eval {
        $context->{attributes}->{allowed_clis} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_CLIS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{allowed_clis}) {
        rowprocessingerror(threadid(),'cannot find allowed_clis attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"allowed_clis attribute found",getlogger(__PACKAGE__));
    }

    eval {
        $context->{attributes}->{cli} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CLI_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cli}) {
        rowprocessingerror(threadid(),'cannot find cli attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"cli attribute found",getlogger(__PACKAGE__));
    }

    eval {
        $context->{attributes}->{ac} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::AC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{ac}) {
        rowprocessingerror(threadid(),'cannot find ac attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"ac attribute found",getlogger(__PACKAGE__));
    }

    eval {
        $context->{attributes}->{cc} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cc}) {
        rowprocessingerror(threadid(),'cannot find cc attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"cc attribute found",getlogger(__PACKAGE__));
    }

    eval {
        $context->{attributes}->{account_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ACCOUNT_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{account_id}) {
        rowprocessingerror(threadid(),'cannot find account_id attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"account_id attribute found",getlogger(__PACKAGE__));
    }

    my $barring_resellernames = [];
    eval {
        $barring_resellernames = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::list_barring_resellernames();
    };
    if ($@) {
        rowprocessingerror(threadid(),'error retrieving barrings',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        $context->{ncos_level_map} = {};
        foreach my $barring_resellername (@$barring_resellernames) {
            my $resellername = _apply_reseller_mapping($barring_resellername->{reseller_name});
            #unless ($resellername) {
            #    rowprocessingerror(threadid(),"empty reseller name detected",getlogger(__PACKAGE__));
            #    $result = 0; #even in skip-error mode..
            #}
            my $barring = $barring_resellername->{barrings};
            $barring = $default_barring unless ($barring);
            $result &= _check_ncos_level($context,$resellername,$barring);
        }
    }

    eval {
        $context->{attributes}->{ncos_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::NCOS_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{ncos_id}) {
        rowprocessingerror(threadid(),'cannot find ncos_id attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        processing_info(threadid(),"ncos_id attribute found",getlogger(__PACKAGE__));
    }

    foreach my $gpp_idx (0..9) {
        my $gpp_attr = $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::GPPx_ATTRIBUTE . $gpp_idx;
        eval {
            $context->{attributes}->{$gpp_attr} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($gpp_attr);
        };
        if ($@ or not defined $context->{attributes}->{$gpp_attr}) {
            rowprocessingerror(threadid(),"cannot find $gpp_attr attribute",getlogger(__PACKAGE__));
            $result = 0; #even in skip-error mode..
        } else {
            processing_info(threadid(),"$gpp_attr attribute found",getlogger(__PACKAGE__));
        }
    }

    return $result;
}

sub _check_ncos_level {
    my ($context,$resellername,$barring) = @_;
    my $result = 1;
    if ($barring ne $default_barring and not exists $barring_profiles->{$resellername}) {
        rowprocessingerror(threadid(),"barring mappings for reseller $resellername missing",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } elsif ($barring ne $default_barring and not exists $barring_profiles->{$resellername}->{$barring}) {
        rowprocessingerror(threadid(),"mappings for barring '" . $barring . "' of reseller $resellername missing",getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    } else {
        my $reseller_id = $context->{reseller_map}->{$resellername}->{id};
        $context->{ncos_level_map}->{$reseller_id} = {} unless exists $context->{ncos_level_map}->{$reseller_id};
        my $level = $barring_profiles->{$resellername}->{$barring};
        unless (exists $context->{ncos_level_map}->{$reseller_id}->{$barring}) {
            if (not defined $level or length($level) == 0) {
                $context->{ncos_level_map}->{$reseller_id}->{$barring} = undef;
            } else {
                eval {
                    $context->{ncos_level_map}->{$reseller_id}->{$barring} = NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_resellerid_level(
                        $reseller_id,$level);
                };
                if ($@ or not defined $context->{ncos_level_map}->{$reseller_id}->{$barring}) {
                    my $err = "cannot find ncos level '$level' of reseller $resellername";
                    if (not defined $context->{_rowcount}) {
                        if ($barring ne $default_barring) {
                            rowprocessingerror(threadid(),$err,getlogger(__PACKAGE__));
                            $result = 0; #even in skip-error mode..
                        } else {
                            rowprocessingwarn(threadid(),$err,getlogger(__PACKAGE__));
                        }
                    } elsif ($skip_errors) {
                        _warn($context, $err);
                    } else {
                        _error($context, $err);
                        $result = 0; #even in skip-error mode..
                    }
                } else {
                    processing_info(threadid(),"ncos level '$level' of reseller $resellername found",getlogger(__PACKAGE__));
                }
            }
        }
    }
    return $result;
}

sub _update_mta_contact {

    my ($context) = @_;

    my $existing_contracts = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_externalid($context->{contract}->{external_id});
    if ((scalar @$existing_contracts) > 0) {
        my $existing_contract = $existing_contracts->[0];
        if ((scalar @$existing_contracts) > 1) {
            _warn($context,(scalar @$existing_contracts) . " existing contracts found, using first contact id $existing_contract->{id}");
        } else {
            _info($context,"existing contract id $existing_contract->{id} found",1);
        }
        $context->{contract}->{id} = $existing_contract->{id};
        $context->{bill_subscriber}->{contract_id} = $context->{contract}->{id};
        $context->{prov_subscriber}->{account_id} = $context->{contract}->{id};
    } else {
        #_warn($context,"no existing contract of contact id $existing_contact->{id} found, will be created");

        $context->{contract}->{contact}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
            $context->{contract}->{contact},
        );
        $context->{contract}->{contact_id} = $context->{contract}->{contact}->{id};
        _info($context,"contact id $context->{contract}->{contact}->{id} created",1);
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

        NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule::append_billing_mappings($context->{db},
            $context->{contract}->{id},
            [{ billing_profile_id => $context->{billing_profile}->{id}, }],
        );

        $context->{contract}->{contract_balance_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
            contract_id => $context->{contract}->{id},
        );

        _info($context,"contract id $context->{contract}->{id} created",1);
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

        _info($context,"subscriber uuid $context->{prov_subscriber}->{uuid} created",1);

        #primary alias
        $context->{aliases}->{primary}->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},
            domain_id => $context->{prov_subscriber}->{domain_id},
            subscriber_id => $context->{prov_subscriber}->{id},
            username => $number->{number},
        );

        my @allowed_clis = ();
        push(@allowed_clis,{ id => set_subscriber_preference($context,
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

        _info($context,"primary alias $number->{number} created",1);

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

    }

    return $result;

}

sub _update_mta_preferences {

    my ($context) = @_;

    my $result = 1;

        if (defined $context->{ncos_level}) {
            $context->{preferences}->{ncos_id} = { id => set_subscriber_preference($context,
                $context->{prov_subscriber}->{id},
                $context->{attributes}->{ncos_id},
                $context->{ncos_level}->{id}), value => $context->{ncos_level}->{id} };
            _info($context,"ncos_id preference set to $context->{ncos_level}->{id} - $context->{ncos_level}->{level}",1);
        }

        if (defined $context->{preferences}->{gpp}) {
            my $gpp_idx = 0;
            foreach my $gpp_val (@{$context->{preferences}->{gpp}}) {
                my $gpp_attr = 'gpp' . $gpp_idx;
                $context->{preferences}->{$gpp_attr} = { id => set_subscriber_preference($context,
                    $context->{prov_subscriber}->{id},
                    $context->{attributes}->{$gpp_attr},
                    $gpp_attr), value => $gpp_attr };
                _info($context,"$gpp_attr preference set to $gpp_val",1);
                $gpp_idx++;
            }
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

            _info($context,"alias $number->{number} created",1);
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

        #test:
        #my $allowed_clis = get_subscriber_preference($context,
        #    $context->{prov_subscriber}->{id},
        #    $context->{attributes}->{allowed_clis});

        #my $voip_numbers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::findby_subscriberid($context->{db},
        #    $context->{bill_subscriber}->{id});

        #my $aliases = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberid_username($context->{db},
        #    $context->{prov_subscriber}->{id},undef);

        #_info($context,(scalar @{$context->{numbers}->{other}}) . " aliases created: " . join(',',(map { $_->{number}; } @{$context->{numbers}->{other}})));
    }
    return $result;
}

sub _provision_mta_susbcriber_init_context {

    my ($context,$subscriber_group) = @_;

    my $result = 1;

    $context->{log_info} = [];
    $context->{log_warning} = [];
    $context->{log_error} = [];

    my $first = $subscriber_group->[0];

    unless (defined $first->{sip_username} and length($first->{sip_username}) > 0) {
        _warn($context,'empty sip_username ignored');
        $result = 0;
    }

    $context->{domain} = $context->{domain_map}->{$first->{domain}};
    my $resellername = _apply_reseller_mapping($first->{reseller_name});
    $context->{reseller} = $context->{reseller_map}->{$first->{reseller_name}};
    $context->{billing_profile} = $context->{reseller}->{billingprofile_map}->{$first->{billing_profile_name}};

    $context->{prov_subscriber} = {};
    $context->{prov_subscriber}->{username} = $first->{sip_username};
    $context->{prov_subscriber}->{password} = $first->{sip_password};
    $context->{prov_subscriber}->{webusername} = $first->{web_username};
    $context->{prov_subscriber}->{webpassword} = $first->{web_password};
    my $webusername = $first->{web_username};

    $context->{prov_subscriber}->{uuid} = create_uuid();
    $context->{prov_subscriber}->{domain_id} = $context->{domain}->{prov_domain}->{id};

    $context->{bill_subscriber} = {};
    $context->{bill_subscriber}->{username} = $first->{sip_username};
    $context->{bill_subscriber}->{domain_id} = $context->{domain}->{id};
    $context->{bill_subscriber}->{uuid} = $context->{prov_subscriber}->{uuid};

    undef $context->{contract};
    #undef $context->{channels};

    my @numbers = ();
    my %number_dupes = ();
    my %contract_dupes = ();
    my %barrings = ();
    #my $voicemail = 0;
    foreach my $subscriber (@$subscriber_group) {
        my $number = $subscriber->{cc} . $subscriber->{ac} . $subscriber->{sn};
        if (not exists $number_dupes{$number}) {
            push(@numbers,{
                cc => $subscriber->{cc},
                ac => $subscriber->{ac},
                sn => $subscriber->{sn},
                number => $number,
                #delta => $subscriber->{delta},
                additional => 0,
                filename => $subscriber->{filename},
            });
            $number_dupes{$number} = 1;
        } else {
            _warn($context,"duplicate number $number ($subscriber->{filename}) ignored");
        }

        if (not exists $contract_dupes{$subscriber->{customer_id}}) {
            if (not $context->{contract}) {
                $context->{contract} = {
                    external_id => $subscriber->{customer_id},
                    create_timestamp => $context->{now},
                    product_id => $context->{sip_account_product}->{id},
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
                        #$contact_hash_field => $subscriber->{contact_hash},
                    },
                };
                $contract_dupes{$subscriber->{customer_id}} = 1;
            } else {
                _warn($context,'non-unique contact data, skipped');
                $context->{nonunique_contacts}->{$context->{prov_subscriber}->{username}} += 1;
                $result = 0;
            }
        }

        unless (defined $context->{prov_subscriber}->{password} and length($context->{prov_subscriber}->{password}) > 0) {
            $context->{prov_subscriber}->{password} = $subscriber->{sip_password};
        }

        unless (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0
            and defined $context->{prov_subscriber}->{webpassword} and length($context->{prov_subscriber}->{webpassword}) > 0) {
            $context->{prov_subscriber}->{webusername} = $subscriber->{web_username};
            $context->{prov_subscriber}->{webpassword} = $subscriber->{web_password};
        }

        unless (defined $webusername and length($webusername) > 0) {
            $webusername = $subscriber->{web_username};
        }

        if (defined $subscriber->{barrings} and length($subscriber->{barrings}) > 0) {
            $barrings{$subscriber->{barrings}} = 1;
        }

    }

    unless (defined $context->{prov_subscriber}->{password} and length($context->{prov_subscriber}->{password}) > 0) {
        my $generated = _generate_sippassword();
        $context->{prov_subscriber}->{password} = $generated;
        _info($context,"empty sip_password, using generated '$generated'",1);
    }

    unless (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0) {
        $context->{prov_subscriber}->{webusername} = $webusername;
        $context->{prov_subscriber}->{webpassword} = undef;
    }

    if (not (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0)) {
        $context->{prov_subscriber}->{webusername} = undef;
        $context->{prov_subscriber}->{webpassword} = undef;
        _info($context,"empty web_username for sip_username '$first->{sip_username}'",1);
    } else {
        $webusername = $context->{prov_subscriber}->{webusername};
        my %webusername_dupes = map { $_->{sip_username} => 1; }
            @{NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::findby_domain_webusername(
            $first->{domain},$webusername)};
        if ((scalar keys %webusername_dupes) > 1) {
            my $generated = _generate_webusername(); #$first->{sip_username};
            _info($context,"duplicate web_username '$webusername', using generated '$generated'",1);
            $context->{prov_subscriber}->{webusername} = $generated;
        }

        #$context->{prov_subscriber}->{webpassword} = $first->{web_password};
        if (not (defined $context->{prov_subscriber}->{webpassword} and length($context->{prov_subscriber}->{webpassword}) > 0)) {
            my $generated = _generate_webpassword();
            _info($context,"empty web_password for web_username '$webusername', using generated '$generated'",1);
            $context->{prov_subscriber}->{webpassword} = $generated;
        #} elsif (defined $first->{web_password} and length($first->{web_password}) < 8) {
        #    $context->{prov_subscriber}->{webpassword} = _generate_webpassword();
        #    _info($context,"web_password for web_username '$first->{web_username}' is too short, using '$context->{prov_subscriber}->{webpassword}'");
        }
    }

    $context->{ncos_level} = undef;
    if ((scalar keys %barrings) > 1) {
        my $combined_barring = join('_',sort keys %barrings);
        #$result &=
        _check_ncos_level($context,$resellername,$combined_barring);
        _info($context,"barrings combination $combined_barring");
        $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$combined_barring};
    } elsif ((scalar keys %barrings) == 1) {
        my ($barring) = keys %barrings;
        $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$barring};
    } else {
        if (exists $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$default_barring}) {
            $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$default_barring};
            _info($context,"no ncos level, using default '$context->{ncos_level}->{level}'",1);
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
    #return 0 unless scalar @{$context->{numbers}->{other}};

    $context->{voip_numbers} = {};
    $context->{voip_numbers}->{primary} = undef;
    $context->{voip_numbers}->{other} = [];
    $context->{aliases} = {};
    $context->{aliases}->{primary} = undef;
    $context->{aliases}->{other} = [];

    $context->{voicemail_user} = {};
    $context->{voicemail_user}->{customer_id} = $context->{prov_subscriber}->{uuid};
    $context->{voicemail_user}->{mailbox} = $context->{numbers}->{primary}->{number};
    $context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));

    $context->{preferences} = {};

    $context->{preferences}->{gpp} = [
        $first->{"_len"},
        $first->{"_cpe_mta_mac_address"},
        $first->{"_cpe_model"},
        $first->{"_cpe_vendor"},
    ];

    return $result;

}


sub _generate_webpassword {
    return String::MkPasswd::mkpasswd(
        -length => $webpassword_length,
        -minnum => 1, -minlower => 1, -minupper => 1, -minspecial => 1,
        -distribute => 1, -fatal => 1,
    );
}

sub _generate_sippassword {
    return createtmpstring($sippassword_length);
}

sub _generate_webusername {
    return createtmpstring($webusername_length);
}

sub _apply_reseller_mapping {
    my $reseller_name = shift;
    #if (defined $reseller_name and exists $reseller_mapping->{$reseller_name}) {
    #    return $reseller_mapping->{$reseller_name};
    #}
    return $reseller_name;
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    push(@{$context->{log_error}},$message) if exists $context->{log_error};
    if ($context->{prov_subscriber}) {
        $message = ($context->{prov_subscriber}->{username} ? $context->{prov_subscriber}->{username} : '<empty sip_username>') . ': ' . $message;
    }
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    push(@{$context->{log_warning}},$message) if exists $context->{log_warning};
    if ($context->{prov_subscriber}) {
        $message = ($context->{prov_subscriber}->{username} ? $context->{prov_subscriber}->{username} : '<empty sip_username>') . ': ' . $message;
    }
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    push(@{$context->{log_info}},$message) if exists $context->{log_info};
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
