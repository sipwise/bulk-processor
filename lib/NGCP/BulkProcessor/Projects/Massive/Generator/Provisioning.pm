package NGCP::BulkProcessor::Projects::Massive::Generator::Provisioning;
use strict;

## no critic

use threads::shared qw();
use Time::HiRes qw(sleep);
use String::MkPasswd qw();
#use List::Util qw();

use Tie::IxHash;

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
);

use NGCP::BulkProcessor::Projects::Massive::Generator::Settings qw(
    $dry
    $skip_errors
    $deadlock_retries

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $provision_subscriber_count
    $webpassword_length
    $webusername_length
    $sipusername_length
    $sippassword_length


    @providers
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
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();
use NGCP::BulkProcessor::Dao::Trunk::kamailio::location qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Subscribers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

use NGCP::BulkProcessor::Projects::Massive::Generator::Preferences qw(
    set_subscriber_preference
    get_subscriber_preference
    clear_subscriber_preferences
    delete_subscriber_preference
    set_allowed_ips_preferences
    cleanup_aig_sequence_ids
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    ping_dbs
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp zerofill); # stringtobool check_ipnet trim);
#use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
use NGCP::BulkProcessor::RandomString qw(createtmpstring);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_subscribers

);

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;
my $STOP = 8;

my $total_count :shared = 0;
#my $db_lock :shared = undef;

sub provision_subscribers {

    my $context = {};
    my $result = _provision_subscribers_create_context($context);

    destroy_dbs();
    if ($result) {
        if ($enablemultithreading and $provision_subscriber_multithreading and $provision_subscriber_count > 1) {
            $context->{subscriber_count} = int($provision_subscriber_count / $provision_subscriber_numofthreads);
            $context->{sn_increment} = $provision_subscriber_numofthreads;
            my %processors = ();
            for (my $i = 0; $i < $provision_subscriber_numofthreads; $i++) {
                $context->{subscriber_count} += ($provision_subscriber_count - $context->{subscriber_count} * $provision_subscriber_numofthreads) if $i == 0;
                _info($context,'starting generator thread ' . ($i + 1) . ' of ' . $provision_subscriber_numofthreads);
                $context->{sn_offset} = $i;
                my $processor = threads->create(\&_provision_subscriber,$context);
                if (!defined $processor) {
                    _info($context,'generator thread ' . ($i + 1) . ' of ' . $provision_subscriber_numofthreads . ' NOT started');
                }
                $processors{$processor->tid()} = $processor;
            }
            local $SIG{'INT'} = sub {
                _info($context,"interrupt signal received");
                $result = 0;
                lock $context->{errorstates};
                $context->{errorstates}->{$context->{tid}} = $STOP;
            };
            while ((scalar keys %processors) > 0) {
                foreach my $processor (values %processors) {
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        _info($context,'generator thread tid ' . $processor->tid() . ' joined');
                    }
                }
                sleep($thread_sleep_secs);
            }

            $result &= (_get_threads_state($context->{errorstates},$context->{tid}) & $COMPLETED) == $COMPLETED;

        } else {

            $context->{subscriber_count} = $provision_subscriber_count;
            $context->{sn_increment} = 1;
            $context->{sn_offset} = 0;
            local $SIG{'INT'} = sub {
                _info($context,"interrupt signal received");
                $context->{errorstates}->{$context->{tid}} = $STOP;
            };
            $result = _provision_subscriber($context);

        }
    }

    return $result;
}

sub _provision_subscriber {

    my $context = shift;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }
    $context->{tid} = $tid;
    $context->{db} = &get_xa_db();

    my $subscriber_count = 0;
    my $broadcast_state;
    while (($broadcast_state = _get_threads_state($context->{errorstates})) == 0
           or
           (($broadcast_state & $ERROR) == 0
           and ($broadcast_state & $STOP) == 0)) {

        last if $subscriber_count >= $context->{subscriber_count};
        $subscriber_count += 1;

        next unless _provision_subscriber_init_context($context);

        my $retry = 1;
        while ($retry > 0) {
            eval {
                $context->{db}->set_transaction_isolation('READ UNCOMMITTED');
                $context->{db}->db_begin();
                #_info($context,"test" . $subscriber_count);
                #die() if (($tid == 1 or $tid == 0) and $subscriber_count == 500);

                if (NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::countby_ccacsn($context->{db},
                        $context->{numbers}->{primary}->{cc},
                        $context->{numbers}->{primary}->{ac},
                        $context->{numbers}->{primary}->{sn},
                    ) == 0) {

                    _create_contact($context);
                    _create_contract($context);
                    {
                        #lock $db_lock; #concurrent writes to voip_numbers causes deadlocks
                        lock $total_count;
                        _create_subscriber($context);
                        _create_aliases($context);
                        $total_count += 1;
                        _info($context,"$total_count subscribers created",($total_count % 10) > 0);
                    }
            #            _update_preferences($context);
            #            _set_registrations($context);
            #            _set_callforwards($context);
            #            #todo: additional prefs, AllowedIPs, NCOS, Callforwards. still thinking wether to integrate it
            #            #in this main provisioning loop, or align it in separate run-modes, according to the files given.
            #
                    } else {
                        _info($context,'subscriber with primary number $context->{numbers}->{primary}->{number} already exists, skipping',1);
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
                if ($err =~ /deadlock/gi and $retry < $deadlock_retries) {
                    my $sleep = 0.01 * 2**$retry;
                    _info($context,"retrying in $sleep secs");
                    sleep($sleep);
                    $retry += 1;
                } elsif (not $skip_errors) {
                    undef $context->{db};
                    destroy_dbs();
                    lock $context->{errorstates};
                    $context->{errorstates}->{$tid} = $ERROR;
                    return 0;
                }
            } else {
                $retry = 0;
            }
        }
    }
    undef $context->{db};
    destroy_dbs();
    if (($broadcast_state & $ERROR) == $ERROR) {
        _info($context,"shutting down (error broadcast)");
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $STOP;
        return 0;
    } elsif (($broadcast_state & $STOP) == $STOP) {
        _info($context,"shutting down (stop broadcast)");
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $STOP;
        return 0;
    } else {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $COMPLETED;
        return 1;
    }

}

sub _provision_subscribers_create_context {
    my ($context) = @_;

    my $result = 1;

    my %errorstates :shared = ();
    my $tid = threadid();
    $context->{tid} = $tid;
    $context->{now} = timestamp();
    $context->{errorstates} = \%errorstates;
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }
    $context->{error_count} = 0;
    $context->{warning_count} = 0;

    my $result = 1;

    if ((scalar @providers) == 0) {
        _error($context,"load/create providers first");
        $result = 0; #even in skip-error mode..
    }

    #$context->{providers}
    foreach my $provider (@providers) {
        unless ($provider->{provider_fee}) {
            _error($context,"no provider fee for reseller '$provider->{reseller}->{name}' found");
            $result = 0; #even in skip-error mode..
        }
        if ((scalar @{$provider->{subscriber_fees}}) == 0) {
            _error($context,"no subscriber fees for reseller '$provider->{reseller}->{name}' found");
            $result = 0; #even in skip-error mode..
        }

        eval {
            $provider->{domain}->{prov_domain} =
                NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($provider->{domain}->{domain});
        };
        if ($@ or not $provider->{domain}->{prov_domain}) {
            _error($context,"cannot find provisioning domain '$provider->{domain}->{domain}'");
            $result = 0; #even in skip-error mode..
        } else {
            _info($context,"provisioning domain '$provider->{domain}->{domain}' found");
        }

        $provider->{numbers_per_subscriber} //= 1;
        $provider->{numbers_per_subscriber} = 1 if $provider->{numbers_per_subscriber} <= 0;
        my ($sn_min,$sn_max) = split(/[: -]+/,$provider->{sn},2);
        my $sn_length = length($sn_min);
        $sn_length = length($sn_max) if length($sn_max) > $sn_length;
        if ($sn_length > 0 and $sn_max > $sn_min and $sn_min >= 0) {
            my @sn_block = map { zerofill($_,$sn_length); } ($sn_min..$sn_max);
            if (($provision_subscriber_count * $provider->{numbers_per_subscriber}) > scalar @sn_block) {
                _error($context,"sn range $provider->{sn} less than numbers needed ($provider->{numbers_per_subscriber} * $provision_subscriber_count)");
                $result = 0; #even in skip-error mode..
            } else {
                $provider->{sn_block} = \@sn_block;
            }
        #$provider->{sn_block_size} = scalar @sn_block;
        } else {
            _error($context,"invalid sn block definition for provider '$provider->{sn}'");
            $result = 0; #even in skip-error mode..
        }
    }

    eval {
        $context->{sip_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];
    };
    if ($@ or not defined $context->{sip_account_product}) {
        _error($context,"cannot find $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE product");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE product found");
    }

    $context->{attributes} = {};

    eval {
        $context->{attributes}->{allowed_clis} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_CLIS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{allowed_clis}) {
        _error($context,'cannot find allowed_clis attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"allowed_clis attribute found");
    }

    eval {
        $context->{attributes}->{cli} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CLI_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cli}) {
        _error($context,'cannot find cli attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"cli attribute found");
    }

    eval {
        $context->{attributes}->{ac} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::AC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{ac}) {
        _error($context,'cannot find ac attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"ac attribute found");
    }

    eval {
        $context->{attributes}->{cc} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CC_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{cc}) {
        _error($context,'cannot find cc attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"cc attribute found");
    }

    eval {
        $context->{attributes}->{account_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ACCOUNT_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{account_id}) {
        _error($context,'cannot find account_id attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"account_id attribute found");
    }

    eval {
        $context->{attributes}->{concurrent_max_total} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CONCURRENT_MAX_TOTAL_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{concurrent_max_total}) {
        _error($context,'cannot find concurrent_max_total attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"concurrent_max_total attribute found");
    }

    eval {
        $context->{attributes}->{clir} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CLIR_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{clir}) {
        _error($context,'cannot find clir attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"clir attribute found");
    }

    eval {
        $context->{attributes}->{allowed_ips_grp} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{allowed_ips_grp}) {
        _error($context,'cannot find allowed_ips_grp attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"allowed_ips_grp attribute found");
    }

    eval {
        $context->{attributes}->{adm_ncos_id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ID_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{adm_ncos_id}) {
        _error($context,'cannot find adm_ncos_id attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"adm_ncos_id attribute found");
    }

    foreach my $cf_attribute (@NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CF_ATTRIBUTES) {
        eval {
            $context->{attributes}->{$cf_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($cf_attribute);
        };
        if ($@ or not defined $context->{attributes}->{$cf_attribute}) {
            _error($context,"cannot find $cf_attribute attribute");
            $result = 0; #even in skip-error mode..
        } else {
            _info($context,"$cf_attribute attribute found");
        }
    }

    eval {
        $context->{attributes}->{ringtimeout} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::RINGTIMEOUT_ATTRIBUTE);
    };
    if ($@ or not defined $context->{attributes}->{ringtimeout}) {
        _error($context,'cannot find ringtimeout attribute');
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"ringtimeout attribute found");
    }

    return $result;

}

#sub _check_ncos_level {
#    my ($context,$resellername,$barring) = @_;
#    my $result = 1;
#    if ($barring ne $default_barring and not exists $barring_profiles->{$resellername}) {
#        _error($context,"barring mappings for reseller $resellername missing");
#        $result = 0; #even in skip-error mode..
#    } elsif ($barring ne $default_barring and not exists $barring_profiles->{$resellername}->{$barring}) {
#        _error($context,"mappings for barring '" . $barring . "' of reseller $resellername missing");
#        $result = 0; #even in skip-error mode..
#    } else {
#        my $reseller_id = $context->{reseller_map}->{$resellername}->{id};
#        $context->{ncos_level_map}->{$reseller_id} = {} unless exists $context->{ncos_level_map}->{$reseller_id};
#        my $level = $barring_profiles->{$resellername}->{$barring};
#        unless (exists $context->{ncos_level_map}->{$reseller_id}->{$barring}) {
#            if (not defined $level or length($level) == 0) {
#                $context->{ncos_level_map}->{$reseller_id}->{$barring} = undef;
#            } else {
#                eval {
#                    $context->{ncos_level_map}->{$reseller_id}->{$barring} = NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_resellerid_level(
#                        $reseller_id,$level);
#                };
#                if ($@ or not defined $context->{ncos_level_map}->{$reseller_id}->{$barring}) {
#                    my $err = "cannot find ncos level '$level' of reseller $resellername";
#                    if (not defined $context->{_rowcount}) {
#                        if ($barring ne $default_barring) {
#                            _error($context,$err);
#                            $result = 0; #even in skip-error mode..
#                        } else {
#                            rowprocessingwarn(threadid(),$err);
#                        }
#                    } elsif ($skip_errors) {
#                        _warn($context, $err);
#                    } else {
#                        _error($context, $err);
#                        $result = 0; #even in skip-error mode..
#                    }
#                } else {
#                    _info($context,"ncos level '$level' of reseller $resellername found");
#                }
#            }
#        }
#    }
#    return $result;
#}

sub _create_contact {

    my ($context) = @_;

    $context->{contract}->{contact}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
        $context->{contract}->{contact},
    );
    $context->{contract}->{contact_id} = $context->{contract}->{contact}->{id};
    _info($context,"contact id $context->{contract}->{contact}->{id} created",1);
    $context->{contract}->{contact_id} = $context->{contract}->{contact}->{id};

    return 1;

}

sub _create_contract {

    my ($context) = @_;
#
#    if ($context->{bill_subscriber}->{contract_id}) {
#        #todo: the update case
#    } else {
#        #the insert case
        $context->{contract}->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
            $context->{contract}
        );
        $context->{bill_subscriber}->{contract_id} = $context->{contract}->{id};
        $context->{prov_subscriber}->{account_id} = $context->{contract}->{id};

        NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule::append_billing_mappings($context->{db},
            $context->{contract}->{id},
            [{ billing_profile_id => $context->{billing_profile}->{id}, }],
        );
        #$context->{contract}->{billing_mapping_id} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::insert_row($context->{db},
        #    billing_profile_id => $context->{billing_profile}->{id},
        #    contract_id => $context->{contract}->{id},
        #    product_id => $context->{sip_account_product}->{id},
        #);

        $context->{contract}->{contract_balance_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
            contract_id => $context->{contract}->{id},
        );

        _info($context,"contract id $context->{contract}->{id} created",1);
    #}
    return 1;

}

sub _create_subscriber {

    my ($context) = @_;

#    my $result = 1;
#
#    if ($context->{bill_subscriber}->{id}) {
#        #todo: the update case
#    } else {
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

#    }

    #return $result;
    return 1;

}

#sub _update_preferences {
#
#    my ($context) = @_;
#
#    my $result = 1;
#
#        if (defined $context->{channels}) {
#            $context->{preferences}->{concurrent_max_total} = { id => set_subscriber_preference($context,
#                $context->{prov_subscriber}->{id},
#                $context->{attributes}->{concurrent_max_total},
#                $context->{channels}), value => $context->{channels} };
#            _info($context,"concurrent_max_total preference set to $context->{channels}",1);
#        }
#
#        if ($context->{clir}) {
#            $context->{preferences}->{clir} = { id => set_subscriber_preference($context,
#                $context->{prov_subscriber}->{id},
#                $context->{attributes}->{clir},
#                $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::TRUE), value => $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::TRUE };
#            _info($context,"clir preference set to $context->{clir}",1);
#        }
#
#        if ((scalar @{$context->{allowed_ips}}) > 0) {
#            my ($allowed_ip_group_preferrence_id, $allowed_ip_group_id) = set_allowed_ips_preferences($context,
#                $context->{prov_subscriber}->{id},
#                $context->{prov_subscriber}->{username},
#                $context->{attributes}->{allowed_ips_grp},
#                $context->{allowed_ips},
#            );
#            $context->{preferences}->{allowed_ips_grp} = { id => $allowed_ip_group_preferrence_id, value => $allowed_ip_group_id };
#            _info($context,"allowed_ips_grp preference set to $allowed_ip_group_id - " . join(',',@{$context->{allowed_ips}}),1);
#        }
#
#        if (defined $context->{ncos_level}) {
#            $context->{preferences}->{adm_ncos_id} = { id => set_subscriber_preference($context,
#                $context->{prov_subscriber}->{id},
#                $context->{attributes}->{adm_ncos_id},
#                $context->{ncos_level}->{id}), value => $context->{ncos_level}->{id} };
#            _info($context,"adm_ncos_id preference set to $context->{ncos_level}->{id} - $context->{ncos_level}->{level}",1);
#        }
#
#
#
#    return $result;
#
#}

sub _create_aliases {

    my ($context) = @_;
#    my $result = 1;

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
    return 1;
}

#sub _set_registrations {
#
#    my ($context) = @_;
#    my $result = 1;
#    foreach my $registration (@{$context->{registrations}}) {
#        #print "blah";
#        $registration->{id} = NGCP::BulkProcessor::Dao::Trunk::kamailio::location::insert_row($context->{db},
#            %$registration);
#        _info($context,"permanent registration $registration->{contact} added",1);
#    }
#    foreach my $trusted_source (@{$context->{trusted_sources}}) {
#        #print "blah";
#        $trusted_source->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources::insert_row($context->{db},{
#            %$trusted_source,
#            subscriber_id => $context->{prov_subscriber}->{id},
#            uuid => $context->{prov_subscriber}->{uuid},
#        });
#        _info($context,"trusted source $trusted_source->{protocol} $trusted_source->{src_ip} from $trusted_source->{from_pattern} added",1);
#    }
#    return $result;
#
#}

#sub _set_callforwards {
#
#    my ($context) = @_;
#    my $result = 1;
#    foreach my $type (keys %{$context->{callforwards}}) {
#        #use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets qw();
#        #use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations qw();
#
#        my $destination_set_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets::insert_row($context->{db},{
#            subscriber_id => $context->{prov_subscriber}->{id},
#            name => "quickset_$type",
#        });
#        foreach my $callforward (@{$context->{callforwards}->{$type}}) {
#            $callforward->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations::insert_row($context->{db},{
#                %$callforward,
#                destination_set_id => $destination_set_id,
#            });
#        }
#        my $cf_mapping_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::insert_row($context->{db},{
#            subscriber_id => $context->{prov_subscriber}->{id},
#            type => $type,
#            destination_set_id => $destination_set_id,
#            #time_set_id
#        });
#
#        $context->{preferences}->{$type} = { id => set_subscriber_preference($context,
#            $context->{prov_subscriber}->{id},
#            $context->{attributes}->{$type},
#            $cf_mapping_id), value => $cf_mapping_id };
#
#        if (defined $context->{ringtimeout}) {
#            $context->{preferences}->{ringtimeout} = { id => set_subscriber_preference($context,
#                $context->{prov_subscriber}->{id},
#                $context->{attributes}->{ringtimeout},
#                $context->{ringtimeout}), value => $context->{ringtimeout} };
#        }
#        _info($context,"$type created (destination(s) " . join(', ',(map { $_->{destination}; } @{$context->{callforwards}->{$type}})) . ")",1);
#
#        $context->{callforwards}->{$type} = {
#            destination_set => {
#                destinations => $context->{callforwards}->{$type},
#                id => $destination_set_id,
#            },
#            id => $cf_mapping_id,
#        };
#    }
#    return $result;
#
#}

sub _provision_subscriber_init_context {

    my ($context) = @_;

    my $result = 1;

    my $provider = $providers[rand @providers];

    $context->{domain} = $provider->{domain};
    $context->{reseller} = $provider->{reseller};

    {
        my @fees = @{$provider->{subscriber_fees}};
        $context->{billing_profile} = $fees[rand @fees]->{profile};
    }

    $context->{prov_subscriber} = {};
    $context->{prov_subscriber}->{username} = _generate_sipusername();
    $context->{prov_subscriber}->{password} = _generate_sippassword();
    $context->{prov_subscriber}->{webusername} = _generate_webusername();
    $context->{prov_subscriber}->{webpassword} = _generate_webpassword();

    $context->{prov_subscriber}->{uuid} = create_uuid();
    $context->{prov_subscriber}->{domain_id} = $context->{domain}->{prov_domain}->{id};

    $context->{bill_subscriber} = {};
    $context->{bill_subscriber}->{username} = $context->{prov_subscriber}->{username};
    $context->{bill_subscriber}->{domain_id} = $context->{domain}->{id};
    $context->{bill_subscriber}->{uuid} = $context->{prov_subscriber}->{uuid};

    $context->{contract} = {
        external_id => undef, #xxx$subscriber->{customer_id},
        create_timestamp => $context->{now},
        product_id => $context->{sip_account_product}->{id},
        contact => {
            reseller_id => $context->{reseller}->{id},

#            firstname => xx$subscriber->{first_name},
#            lastname => $subscriber->{last_name},
#            compregnum => $subscriber->{company_registration_number},
#            company => $subscriber->{company},
#            street => $subscriber->{street},
#            postcode => $subscriber->{postal_code},
#            city => $subscriber->{city_name},
#            #country => $context->{contract}->{contact}->{country},
#            phonenumber => $subscriber->{phone_number},
#            email => $subscriber->{email},
#            vatnum => $subscriber->{vat_number},
#            #$contact_hash_field => $subscriber->{contact_hash},
        },
    };

    $context->{channels} = undef; #$default_channels;
    $context->{allowed_ips} = []; #[ keys %allowed_ips ];
    $context->{ncos_level} = undef;

    my @numbers = ();
    foreach (1..$provider->{numbers_per_subscriber}) {
        my $number = {};
        my @cc = @{$provider->{cc}};
        $number->{cc} = $cc[rand @cc];
        my @ac = @{$provider->{ac}};
        $number->{ac} = $ac[rand @ac];
        $number->{sn} = $provider->{sn_block}->[$context->{sn_offset}];
        $context->{sn_offset} += $context->{sn_increment};
        $number->{number} = ($number->{cc} // '') . ($number->{ac} // '') . ($number->{sn} // '');
        push(@numbers,$number);
    }

    $context->{numbers} = {};
    $context->{numbers}->{other} = \@numbers; #sort_by_configs(\@numbers,[
#        {   numeric     => 1,
#            dir         => 1, #-1,
#            memberchain => [ 'additional' ],
#        },
#        {   numeric     => 0,
#            dir         => 1, #-1,
#            memberchain => [ 'cc' ],
#        },
#        {   numeric     => 0,
#            dir         => 1, #-1,
#            memberchain => [ 'ac' ],
#        },
#        {   numeric     => 0,
#            dir         => 1, #-1,
#            memberchain => [ 'sn' ],
#        },
#    ]);
    $context->{numbers}->{primary} = shift(@{$context->{numbers}->{other}});
    #return 0 unless scalar @{$context->{numbers}->{other}};

    #if ($number_for_sipusername) {
    #    $context->{prov_subscriber}->{username} = $context->{numbers}->{primary}->{number};
    #    $context->{bill_subscriber}->{username} = $context->{numbers}->{primary}->{number};
    #}

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
    $context->{clir} = 0;

    $context->{ringtimeout} = undef;
    my %cfsimple = ();
#                push(@{$cfsimple{$type}},{
#                    destination => $callforward->{destination},
#                    priority => $callforward->{priority},
#                    timeout => $callforward->{timeout},
#                });
#
    $context->{callforwards} = \%cfsimple;

    my @registrations = ();
    my @trusted_sources = ();
#        push(@registrations,{
#            username => $registration->{sip_username},
#            domain => $registration->{domain},
#            contact => 'sip:' . $registration->{sip_contact},
#            ruid => NGCP::BulkProcessor::Dao::Trunk::kamailio::location::next_ruid(),
#        });
#        if ($registration->{sip_contact} =~ /(\d{0,3}\.\d{0,3}\.\d{0,3}\.\d{0,3})/) {
#           if (check_ipnet($1)) {
#                push(@trusted_sources,{
#                    src_ip => $1,
#                    protocol => $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources::PROTOCOL_UDP,
#                    from_pattern => 'sip:.+' . quotemeta($context->{domain}->{domain}),
#                });
#
    $context->{registrations} = \@registrations;
    $context->{trusted_sources} = \@trusted_sources;

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

sub _generate_sipusername {
    return createtmpstring($sipusername_length);
}

sub _get_threads_state {
    my ($errorstates,$tid) = @_;
    my $result = 0;
    if (defined $errorstates and ref $errorstates eq 'HASH') {
        lock $errorstates;
        foreach my $threadid (keys %$errorstates) {
            if (not defined $tid or $threadid != $tid) {
                $result |= $errorstates->{$threadid};
            }
        }
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
