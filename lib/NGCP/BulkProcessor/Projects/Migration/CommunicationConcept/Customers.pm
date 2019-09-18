package NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Customers;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Settings qw(
    $dry
    $skip_errors
    $report_filename

    $copy_contract_multithreading
    $copy_contract_numofthreads
    $copy_contract_blocksize

    run_dao_method
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


use NGCP::BulkProcessor::Dao::mr341::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr341::billing::resellers qw();
use NGCP::BulkProcessor::Dao::mr341::provisioning::voip_usr_preferences qw();

use NGCP::BulkProcessor::Dao::mr553::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr553::billing::resellers qw();
use NGCP::BulkProcessor::Dao::mr553::provisioning::voip_usr_preferences qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();


use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_sound_sets qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();
#use NGCP::BulkProcessor::Dao::Trunk::kamailio::location qw();

#use NGCP::BulkProcessor::RestRequests::Trunk::Subscribers qw();
#use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Preferences qw(
    cleanup_aig_sequence_ids
    create_usr_preferences
    check_replaced_prefs
    map_preferences
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::ProjectConnectorPool qw(
    get_source_accounting_db
    get_source_billing_db
    get_source_provisioning_db
    get_source_kamailio_db
    destroy_all_dbs
    ping_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp stringtobool trim); #check_ipnet
use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
#use NGCP::BulkProcessor::RandomString qw(createtmpstring);
use NGCP::BulkProcessor::Table qw(get_rowhash);
use NGCP::BulkProcessor::Array qw(array_to_map);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    copy_customers
);

#my $db_lock :shared = undef;
#my $file_lock :shared = undef;

#my $default_barring = 'default';

#my $ccs_contact_identifier_field = 'gpp9';

my $contact_hash_field = 'gpp9';

sub copy_customers {

    my $static_context = {
        source_dbs => {
            billing_db => \&get_source_billing_db,
            provisioning_db => \&get_source_provisioning_db,
            kamailio_db => \&get_source_kamailio_db,
        },
    };
    my $result = _copy_customers_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && run_dao_method('billing::contracts::source_process_records',
        source_dbs => $static_context->{source_dbs},
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            $context->{cleanup_aig_sequence} = 0;
            foreach my $contract (@$records) {
                #$context->{_rowcount} += 1;
                next unless _copy_contract_init_context($context,$contract);
                next unless _copy_contract($context);
            #    push(@report_data,_get_report_obj($context));
            }
            if ($context->{cleanup_aig_sequence}) {
                cleanup_aig_sequence_ids($context);
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
            #{
            #    lock %nonunique_contacts;
            #    foreach my $sip_username (keys %{$context->{nonunique_contacts}}) {
            #        $nonunique_contacts{$sip_username} = $context->{nonunique_contacts}->{$sip_username};
            #    }
            #}
        },
        destroy_reader_dbs_code => \&destroy_all_dbs,
        blocksize => $copy_contract_blocksize,
        multithreading => $copy_contract_multithreading,
        numofthreads => $copy_contract_numofthreads,
    ),$warning_count,);

}

sub _copy_contract {
    my ($context) = @_;

    eval {
        #lock $db_lock;
        $context->{db}->db_begin();
        #_warn($context,'AutoCommit is on') if $context->{db}->{drh}->{AutoCommit};

        (my $subscriber_map,my $domain_ids,my $usernames) = array_to_map($context->{voip_subscribers},
            sub { return shift->{domain_id}; }, sub { return shift->{username}; }, 'group' );
        my @existing_billing_voip_subscribers = ();
        foreach my $domain_id (keys %$subscriber_map) {
            push(@existing_billing_voip_subscribers,@{NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_usernames(
                $context->{db},
                $context->{domain_id_map}->{$domain_id}->{billing_domain_id},
                $subscriber_map->{$domain_id},
            )});
        }

        if ((scalar @existing_billing_voip_subscribers) > 0) {
            _warn($context,"contract with subscriber(s) " . join(',',map { $_->{username}; } @existing_billing_voip_subscribers) . " already exists, skipping");
        } else {
            _create_contract_contact($context);
            _create_contract($context);
            _create_subscribers($context);
            #_create_reseller($context);
            #_create_billing_profiles($context);
            #_create_domains($context);
            #_create_email_templates($context);

            #$result = 1;
            #print "blah";
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

sub _copy_customers_checks {
    my ($context) = @_;

    my $result = 1;

    $context->{reseller_id_map} = {};
    $context->{domain_id_map} = {};
    $context->{billing_profile_id_map} = {};
    $context->{attribute_map} = {};
    eval {
        foreach my $old_reseller (@{run_dao_method('billing::resellers::source_findall',$context->{source_dbs})}) {
            my $new_reseller = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name($old_reseller->{name});
            die("reseller '$old_reseller->{name}' not found") unless $new_reseller;
            $context->{reseller_id_map}->{$old_reseller->{id}} = $new_reseller->{id};
            foreach my $old_domain (@{$old_reseller->{domains}}) {
                my $new_billing_domain = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($old_domain->{domain});
                die("billing domain '$old_domain->{domain}' not found") unless $new_billing_domain;
                my $new_provisioning_domain = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($old_domain->{domain});
                die("provisioning domain '$old_domain->{domain}' not found") unless $new_provisioning_domain;
                $context->{domain_id_map}->{$old_domain->{id}} = {
                    billing_domain_id => $new_billing_domain->{id},
                    provisioning_domain_id => $new_provisioning_domain->{id},
                };
            }
            foreach my $old_billing_profile (@{$old_reseller->{billing_profiles}}) {
                my $new_billing_profile = NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findby_resellerid_name_handle($new_reseller->{id},
                    $old_billing_profile->{name},
                )->[0];
                die("billing profile '$old_billing_profile->{name}' not found") unless $new_billing_profile;
                $context->{billing_profile_id_map}->{$old_billing_profile->{id}} = $new_billing_profile->{id};
            }
        }
    };
    if ($@) {
        _error($context,$@);
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,(scalar keys %{$context->{reseller_id_map}}) . " resellers mapped");
        _info($context,(scalar keys %{$context->{billing_profile_id_map}}) . " billing profiles mapped");
        _info($context,(scalar keys %{$context->{domain_id_map}}) . " domains mapped");
    }

    $result &= map_preferences($context);

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

    eval {
        $context->{default_billing_profile} = NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::findby_resellerid_name_handle(undef,undef,
            $NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::DEFAULT_PROFILE_HANDLE)->[0];
    };
    if ($@ or not defined $context->{default_billing_profile}) {
        _error($context,"cannot find default billing profile");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"default billing profile found");
    }

    eval {
        foreach my $attribute (@{run_dao_method('provisioning::voip_usr_preferences::source_findby_attributesused',$context->{source_dbs})}) {
            (my $obsolete, my $replacements) = check_replaced_prefs($attribute);
            if ($obsolete) {
                foreach my $replacement (@$replacements) {
                    my $preferrence = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($replacement);
                    die("replacement preferrence '$replacement' not found") unless $preferrence;
                    $context->{attribute_map}->{$replacement} = $preferrence;
                }
                _info($context,"preference '$attribute' will be replaced");
            } else {
                my $preferrence = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($attribute);
                die("preferrence '$attribute' not found") unless $preferrence;
                $context->{attribute_map}->{$attribute} = $preferrence;
            }
        }
        @{$context->{attribute_map}}{keys %NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::DPID_ATTRIBUTES} =
            map { NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($_); } keys %NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::DPID_ATTRIBUTES;
    };
    if ($@) {
        _error($context,$@);
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,(scalar keys %{$context->{attribute_map}}) . " preferrences mapped");
    }

    return $result;
}

sub _create_contract_contact {

    my ($context) = @_;

    my $existing_contacts = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_fields($context->{db},{
        $contact_hash_field => $context->{contact}->{$contact_hash_field},
    });
    if ((scalar @$existing_contacts) > 0) {
        my $existing_contact = $existing_contacts->[0];
        if ((scalar @$existing_contacts) > 1) {
            _warn($context,(scalar @$existing_contacts) . " existing contacts found, using first contact id $existing_contact->{id}");
        } else {
            _info($context,"existing customer contact id $existing_contact->{id} found",1);
        }
        $context->{contract}->{contact_id} = $existing_contact->{id};
    } else {
        $context->{contract}->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
            $context->{contact},
        );
        _info($context,"customer contact id $context->{contract}->{contact_id} created",1);
    }

    return 1;

}

sub _create_subscriber_contact {

    my ($context,$billing_subscriber,$c) = @_;

    if ($c) {
        my $contact = { %$c };
        delete $contact->{id};
        $contact->{reseller_id} = $context->{contact}->{reseller_id};
        my @contact_fields = grep { $_ ne 'id' and $_ ne $contact_hash_field and not ref $contact->{$_}; } sort keys %$contact;
        $contact->{$contact_hash_field} = get_rowhash([@{$contact}{@contact_fields}]);
        my $existing_contacts = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_fields($context->{db},{
            $contact_hash_field => $contact->{$contact_hash_field},
        });
        if ((scalar @$existing_contacts) > 0) {
            my $existing_contact = $existing_contacts->[0];
            if ((scalar @$existing_contacts) > 1) {
                _warn($context,(scalar @$existing_contacts) . " existing contacts found, using first contact id $existing_contact->{id}");
            } else {
                _info($context,"existing subscriber contact id $existing_contact->{id} found",1);
            }
            $billing_subscriber->{contact_id} = $existing_contact->{id};
        } else {
            $billing_subscriber->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
                $contact,
            );
            _info($context,"subscriber contact id $billing_subscriber->{contact_id} created",1);
        }
    } else {
        if ($billing_subscriber->{contact_id}) {
            _warn($context,"missing subscriber contact for subscriber $billing_subscriber->{uuid}");
        }
        $billing_subscriber->{contact_id} = undef;
    }

    return 1;

}

sub _create_contract {

    my ($context) = @_;

    #my $old_contract_id = $context->{peer_group}->{peering_contract_id};
    $context->{contract_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($context->{db},
        $context->{contract},
    );
    _info($context,"customer contract id $context->{contract_id} created",1);

    #$context->{bill_subscriber}->{contract_id} = $context->{contract}->{id};
    #$context->{prov_subscriber}->{account_id} = $context->{contract}->{id};

    foreach my $cb (@{$context->{contract_balances}}) {
        my $contract_balance = { %$cb };
        delete $contract_balance->{id};
        delete $contract_balance->{invoice_id};
        $contract_balance->{contract_id} = $context->{contract_id};
        $contract_balance->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($context->{db},
            $contract_balance,
        );
    }
    _info($context,"customer contract id $context->{contract_id} contract balances created",1);

    #$context->{deferred_billing_mappings}->{$context->{reseller}->{contract_id}} = $context->{billing_mappings};
    _append_billing_mappings($context,$context->{contract_id},
        $context->{billing_mappings},
        $context->{contract});

    return 1;

}

sub _append_billing_mappings {
    my ($context,$contract_id,$billing_mappings,$contract) = @_;
    my @mappings = ();
    foreach my $bm (@{sort_by_configs($billing_mappings,[
            {   numeric     => 1,
                dir         => 1, #-1,
                memberchain => [ 'id' ],
            }
        ])}) {
        my $mapping = { %$bm };
        #$mapping->{contract_id} = $contract_id;
        #_info($context,"creating old system contract id $mapping->{contract_id} billing mappings");
        if ($mapping->{billing_profile_id}
            and exists $context->{billing_profile_id_map}->{$mapping->{billing_profile_id}}) {
            $mapping->{billing_profile_id} = $context->{billing_profile_id_map}->{$mapping->{billing_profile_id}};
        } else {
            $mapping->{billing_profile_id} = $context->{default_billing_profile}->{id};
            _warn($context,"invalid billing mapping for customer contract id $contract_id. using billing profile id $mapping->{billing_profile_id}.");
        }
        push(@mappings,$mapping);
    }
    if ((scalar @mappings) == 0) {
        _warn($context,"no billing mappings for customer contract id $contract_id");
        push(@mappings,{
            billing_profile_id => $context->{default_billing_profile}->{id},
            start_date => undef,
            end_date => undef,
        });
    } elsif ((scalar @mappings) == 1) {
        my $mapping = $mappings[0];
        if ($mapping->{start_date} or $mapping->{end_date}) {
            _warn($context,"invalid billing mapping for customer contract id $contract_id. clearing start_date, end_date.");
            $mapping->{start_date} = undef;
            $mapping->{end_date} = undef;
        }
    } else {
        my $first = 1;
        foreach my $mapping (@mappings) {
            if (not $mapping->{start_date} and not $mapping->{end_date}) {
                if ($first) {
                    $first = 0;
                } else {
                    _warn($context,"invalid billing mapping for customer contract id $contract_id. using start_date $contract->{modify_timestamp}.");
                    $mapping->{start_date} = $contract->{create_timestamp};
                }
            }
        }
    }
    NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule::append_billing_mappings($context->{db},
        $contract_id,
        \@mappings,
    );
    _info($context,"customer contract id $contract_id billing mappings created",1);
}


sub _create_subscribers {

    my ($context) = @_;

    my $result = 1;

    foreach my $bs (@{$context->{voip_subscribers}}) {
        $context->{db}->db_do("savepoint newsubscriber");
        my $bill_subscriber = { %$bs };
        if ($bill_subscriber->{status} eq $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE) {
            _info($context,"skipping terminated subscriber id $bs->{uuid}",1);
            next;
        }
        $bill_subscriber->{contract_id} = $context->{contract_id};
        $bill_subscriber->{domain_id} = $context->{domain_id_map}->{$bs->{domain_id}}->{billing_domain_id};
        unless (_create_subscriber_contact($context,$bill_subscriber,delete $bill_subscriber->{contact})) {
            $result = 0;
            next;
        }
        delete $bill_subscriber->{id};
        my $old_primary_number_id = delete $bill_subscriber->{primary_number_id};
        my $ps = delete $bill_subscriber->{provisioning_voip_subscriber};
        my $vns = delete $bill_subscriber->{voip_numbers};
        my $pn = delete $bill_subscriber->{primary_number};

        $bill_subscriber->{id} = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::insert_row($context->{db},
            $bill_subscriber,
        );

        _info($context,"billing subscriber uuid $bill_subscriber->{uuid} created",1);

        my $new_primary_number_id;
        my %numbers = ();
        my $number_in_use = 0;
        my $primary_number;
        foreach my $vn (@$vns) {
            my $voip_number = { %$vn };
            my $old_id = delete $voip_number->{id};
            $voip_number->{reseller_id} = $context->{contact}->{reseller_id};
            $voip_number->{subscriber_id} = $bill_subscriber->{id};
            my $new_number_id = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
                $voip_number,
                1,
            );
            my $number = $voip_number->{cc} . $voip_number->{ac} . $voip_number->{sn};
            unless ($new_number_id) {
                _warn($context,"number $number already in use, skipping");
                $number_in_use = 1;
                last;
            }
            $numbers{$number} = {
                number => $number,
                cc => $voip_number->{cc},
                ac => $voip_number->{ac},
                sn => $voip_number->{sn},
                is_primary => 0,
            };
            if ($old_primary_number_id == $old_id) {
                $new_primary_number_id = $new_number_id;
                $numbers{$number}->{is_primary} = 1;
                $primary_number = $numbers{$number};
            }
        }
        if ($number_in_use) {
            $context->{db}->db_do("rollback to savepoint newsubscriber");
            next;
        }
        unless ($new_primary_number_id) {
            my $voip_number = { %$pn };
            delete $voip_number->{id};
            $voip_number->{reseller_id} = $context->{contact}->{reseller_id} if $voip_number->{reseller_id};
            $voip_number->{subscriber_id} = $bill_subscriber->{id} if $voip_number->{subscriber_id};
            $new_primary_number_id = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::insert_row($context->{db},
                $voip_number,
                1, #insert ignore
            );
            #unless ($new_primary_number_id) {
            #    $new_primary_number_id = NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::findby_cc_ac_sn($context->{db},
            #        $voip_number->{cc},
            #        $voip_number->{ac},
            #        $voip_number->{sn},
            #    )->{id};
            #}
            my $number = $voip_number->{cc} . $voip_number->{ac} . $voip_number->{sn};
            if ($new_primary_number_id) {
                $numbers{$number} = {
                    number => $number,
                    cc => $voip_number->{cc},
                    ac => $voip_number->{ac},
                    sn => $voip_number->{sn},
                    is_primary => 1,
                };
                $primary_number = $numbers{$number};
            } else {
                _warn($context,"number $number already in use, skipping");
                $number_in_use = 1;
            }
        }

        if ($number_in_use) {
            $context->{db}->db_do("rollback to savepoint newsubscriber");
            next;
        }

        _info($context,"voip numbers " . join(', ',keys %numbers) . " for subscriber uuid $bill_subscriber->{uuid} created",1);

        NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::update_row($context->{db},{
            id => $bill_subscriber->{id},
            primary_number_id => $new_primary_number_id,
        });

        if ($ps) {
            my $prov_subscriber = { %$ps };
            $prov_subscriber->{account_id} = $context->{contract_id};
            $prov_subscriber->{domain_id} = $context->{domain_id_map}->{$bs->{domain_id}}->{provisioning_domain_id};
            delete $prov_subscriber->{id};
            my $profile_id = delete $prov_subscriber->{profile_id};
            my $profile_set_id = delete $prov_subscriber->{profile_set_id};
            my $aliases = delete $prov_subscriber->{voip_dbaliases};
            my $preferences = delete $prov_subscriber->{voip_usr_preferences};
            my $voicemail_users = delete $prov_subscriber->{voicemail_users};
            my $trusted_sources = delete $prov_subscriber->{trusted_sources};
            $prov_subscriber->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::insert_row($context->{db},
                $prov_subscriber,
            );
            _info($context,"provisioning subscriber uuid $prov_subscriber->{uuid} created",1);
            my $alias_in_use = 0;
            foreach my $alias (@$aliases) {
                my $voip_alias = { %$alias };
                delete $voip_alias->{id};
                $voip_alias->{subscriber_id} = $prov_subscriber->{id};
                $voip_alias->{domain_id} = $context->{domain_id_map}->{$bs->{domain_id}}->{provisioning_domain_id};
                my $voip_alias_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},
                    $voip_alias,
                    1, #insert ignore
                );
                if ($voip_alias_id) {
                    if (my $primary_number = delete $numbers{$voip_alias->{username}}) {
                        if ($primary_number->{is_primary} != $voip_alias->{is_primary}) {
                            _warn($context,"wrong primary alias $voip_alias->{username}");
                        }
                    } else {
                        _warn($context,"no voip number for alias $voip_alias->{username}");
                    }
                } else {
                    _warn($context,"alias $voip_alias->{username} already in use, skipping");
                    $alias_in_use = 1;
                    last;
                }
            }
            if ($alias_in_use) {
                $context->{db}->db_do("rollback to savepoint newsubscriber");
                next;
            } else {
                if (scalar keys %numbers) {
                    _warn($context,"no aliases for voip numbers " . join(", ", keys %numbers));
                }
                _info($context,"dbaliases " . join(', ',map { $_->{username}; } @$aliases) . " for subscriber uuid $prov_subscriber->{uuid} created",1);
            }
            unless (_create_voicemail_users($context,$prov_subscriber,$voicemail_users)) {
                $result = 0;
                #next;
            }
            unless (create_usr_preferences($context,$prov_subscriber,$preferences)) {
                $result = 0;
                #next;
            }
            unless (_create_trusted_sources($context,$prov_subscriber,$trusted_sources)) {
                $result = 0;
                #next;
            }
        }



        #$context->{preferences}->{cli} = { id => set_subscriber_preference($context,
        #    $context->{prov_subscriber}->{id},
        #    $context->{attributes}->{cli},
        #    $number->{number}), value => $number->{number} };
        #
        #_info($context,"subscriber uuid $context->{prov_subscriber}->{uuid} created",1);
        #
        ##primary alias
        #$context->{aliases}->{primary}->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::insert_row($context->{db},
        #    domain_id => $context->{prov_subscriber}->{domain_id},
        #    subscriber_id => $context->{prov_subscriber}->{id},
        #    username => $number->{number},
        #);
        #
        #my @allowed_clis = ();
        #push(@allowed_clis,{ id => set_subscriber_preference($context,
        #    $context->{prov_subscriber}->{id},
        #    $context->{attributes}->{allowed_clis},
        #    $number->{number}), value => $number->{number}});
        #$context->{preferences}->{allowed_clis} = \@allowed_clis;
        #
        #NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers::release_subscriber_numbers($context->{db},
        #    $context->{bill_subscriber}->{id},{ 'NOT IN' => $context->{voip_numbers}->{primary}->{id} });
        #
        #NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::delete_dbaliases($context->{db},
        #    $context->{prov_subscriber}->{id},{ 'NOT IN' => $number->{number} });
        #
        #clear_subscriber_preferences($context,
        #    $context->{prov_subscriber}->{id},
        #    $context->{attributes}->{allowed_clis},
        #    $number->{number});
        #
        #_info($context,"primary alias $number->{number} created",1);
        #
        #$context->{voicemail_user}->{id} = NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::insert_row($context->{db},
        #    $context->{voicemail_user},
        #);
        #
        #$context->{preferences}->{account_id} = { id => set_subscriber_preference($context,
        #    $context->{prov_subscriber}->{id},
        #    $context->{attributes}->{account_id},
        #    $context->{contract}->{id}), value => $context->{contract}->{id} };
        #
        #if (length($number->{ac}) > 0) {
        #    $context->{preferences}->{ac} = { id => set_subscriber_preference($context,
        #        $context->{prov_subscriber}->{id},
        #        $context->{attributes}->{ac},
        #        $number->{ac}), value => $number->{ac} };
        #}
        #if (length($number->{cc}) > 0) {
        #    $context->{preferences}->{cc} = { id => set_subscriber_preference($context,
        #        $context->{prov_subscriber}->{id},
        #        $context->{attributes}->{cc},
        #        $number->{cc}), value => $number->{cc} };
        #}

    }

    return $result;

}

sub _create_voicemail_users {

    my ($context,$prov_subscriber,$vus) = @_;

    foreach my $vu (@$vus) {
        my $voicemail_user = { %$vu };
        delete $voicemail_user->{uniqueid};
        #$context->{voicemail_user}->{customer_id} = $context->{prov_subscriber}->{uuid};
        #$context->{voicemail_user}->{mailbox} = $context->{numbers}->{primary}->{number};
        #$context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));
        $voicemail_user->{uniqueid} = NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::insert_row($context->{db},
            $voicemail_user,
        );
    }
    if (scalar @$vus) {
        _info($context,"voicemail user(s) " . join(', ',map { $_->{mailbox}; } @$vus) . " for subscriber uuid $prov_subscriber->{uuid} created",1);
    } else {
        _warn($context,"no voicemail user for subscriber uuid $prov_subscriber->{uuid}");
    }

}

sub _create_trusted_sources {

    my ($context,$prov_subscriber,$tss) = @_;

    foreach my $ts (@$tss) {
        my $trusted_source = { %$ts };
        delete $trusted_source->{id};
        $trusted_source->{subscriber_id} = $prov_subscriber->{id};
        #$context->{voicemail_user}->{mailbox} = $context->{numbers}->{primary}->{number};
        #$context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));
        $trusted_source->{id} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources::insert_row($context->{db},
            $trusted_source,
        );
    }
    if (scalar @$tss) {
        _info($context,"trusted source(s) " . join(', ',map { $_->{src_ip}; } @$tss) . " for subscriber uuid $prov_subscriber->{uuid} created",1);
    #} else {
    #    _warn($context,"no trusted_source for subscriber uuid $prov_subscriber->{uuid}");
    }

}

sub _copy_contract_init_context {

    my ($context,$contract) = @_;

    my $result = 1;

    #$result = 0 unless scalar @{$contract->{contact}->{reseller}};
    #my @contact_fields = grep { $_ ne 'id' and $_ ne $contact_hash_field and not ref $contract->{$_}; } sort keys %{$contract->{contact}};
    #$contract->{contact}->{$contact_hash_field} = get_rowhash([@{$contract->{contact}}{@contact_fields}]);

    $context->{contract_id} = undef;

    $context->{old_contract} = $contract;
    $context->{contract} = { %$contract };
    delete $context->{contract}->{id};
    $context->{contract}->{product_id} = $context->{sip_account_product}->{id};
    $context->{billing_mappings} = delete $context->{contract}->{billing_mappings};
    $context->{contract_balances} = delete $context->{contract}->{contract_balances};
    $context->{voip_subscribers} = delete $context->{contract}->{voip_subscribers};
    unless (scalar @{$context->{voip_subscribers}}) {
        _info($context,"contract with no subscribers, skipping",1);
        $result = 0;
    }
    $context->{contact} = { %{delete $context->{contract}->{contact}} };
    delete $context->{contact}->{id};
    $context->{contact}->{reseller_id} = $context->{reseller_id_map}->{$context->{contact}->{reseller_id}};
    #$result = 0 unless $contract->{contact}->{reseller};
    my @contact_fields = grep { $_ ne 'id' and $_ ne $contact_hash_field and not ref $context->{contact}->{$_}; } sort keys %{$context->{contact}};
    $context->{contact}->{$contact_hash_field} = get_rowhash([@{$context->{contact}}{@contact_fields}]);


    #$result = 0;
    #foreach my $s (@{$context->{voip_subscribers}}) {
    #    #return 1 if $s->{uuid} eq '8ca8f122-b031-410e-a595-f273bd3ad016';
    #    return 1 if $s->{uuid} eq '8ca8f122-b031-410e-a595-f273bd3ad016';
    #    return 1 if $s->{uuid} eq '846f8530-33a1-4572-b393-4a9f207e11ec';
    #}





    #$context->{log_info} = [];
    #$context->{log_warning} = [];
    #$context->{log_error} = [];
    #
    #my $first = $subscriber_group->[0];
    #
    #unless (defined $first->{sip_username} and length($first->{sip_username}) > 0) {
    #    _warn($context,'empty sip_username ignored');
    #    $result = 0;
    #}
    #
    #$context->{domain} = $context->{domain_map}->{$first->{domain}};
    #my $resellername = _apply_reseller_mapping($first->{reseller_name});
    #$context->{reseller} = $context->{reseller_map}->{$first->{reseller_name}};
    #$context->{billing_profile} = $context->{reseller}->{billingprofile_map}->{$first->{billing_profile_name}};
    #
    #$context->{prov_subscriber} = {};
    #$context->{prov_subscriber}->{username} = $first->{sip_username};
    #$context->{prov_subscriber}->{password} = $first->{sip_password};
    #$context->{prov_subscriber}->{webusername} = $first->{web_username};
    #$context->{prov_subscriber}->{webpassword} = $first->{web_password};
    #my $webusername = $first->{web_username};
    #
    #$context->{prov_subscriber}->{uuid} = create_uuid();
    #$context->{prov_subscriber}->{domain_id} = $context->{domain}->{prov_domain}->{id};
    #
    #$context->{bill_subscriber} = {};
    #$context->{bill_subscriber}->{username} = $first->{sip_username};
    #$context->{bill_subscriber}->{domain_id} = $context->{domain}->{id};
    #$context->{bill_subscriber}->{uuid} = $context->{prov_subscriber}->{uuid};
    #
    #undef $context->{contract};
    ##undef $context->{channels};
    #
    #my @numbers = ();
    #my %number_dupes = ();
    #my %contract_dupes = ();
    #my %barrings = ();
    ##my $voicemail = 0;
    #foreach my $subscriber (@$subscriber_group) {
    #    my $number = $subscriber->{cc} . $subscriber->{ac} . $subscriber->{sn};
    #    if (not exists $number_dupes{$number}) {
    #        push(@numbers,{
    #            cc => $subscriber->{cc},
    #            ac => $subscriber->{ac},
    #            sn => $subscriber->{sn},
    #            number => $number,
    #            #delta => $subscriber->{delta},
    #            additional => 0,
    #            filename => $subscriber->{filename},
    #        });
    #        $number_dupes{$number} = 1;
    #    } else {
    #        _warn($context,"duplicate number $number ($subscriber->{filename}) ignored");
    #    }
    #
    #    if (not exists $contract_dupes{$subscriber->{customer_id}}) {
    #        if (not $context->{contract}) {
    #            $context->{contract} = {
    #                external_id => $subscriber->{customer_id},
    #                create_timestamp => $context->{now},
    #                product_id => $context->{sip_account_product}->{id},
    #                contact => {
    #                    reseller_id => $context->{reseller}->{id},
    #
    #                    firstname => $subscriber->{first_name},
    #                    lastname => $subscriber->{last_name},
    #                    compregnum => $subscriber->{company_registration_number},
    #                    company => $subscriber->{company},
    #                    street => $subscriber->{street},
    #                    postcode => $subscriber->{postal_code},
    #                    city => $subscriber->{city_name},
    #                    #country => $context->{contract}->{contact}->{country},
    #                    phonenumber => $subscriber->{phone_number},
    #                    email => $subscriber->{email},
    #                    vatnum => $subscriber->{vat_number},
    #                    #$contact_hash_field => $subscriber->{contact_hash},
    #                },
    #            };
    #            $contract_dupes{$subscriber->{customer_id}} = 1;
    #        } else {
    #            _warn($context,'non-unique contact data, skipped');
    #            $context->{nonunique_contacts}->{$context->{prov_subscriber}->{username}} += 1;
    #            $result = 0;
    #        }
    #    }
    #
    #    unless (defined $context->{prov_subscriber}->{password} and length($context->{prov_subscriber}->{password}) > 0) {
    #        $context->{prov_subscriber}->{password} = $subscriber->{sip_password};
    #    }
    #
    #    unless (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0
    #        and defined $context->{prov_subscriber}->{webpassword} and length($context->{prov_subscriber}->{webpassword}) > 0) {
    #        $context->{prov_subscriber}->{webusername} = $subscriber->{web_username};
    #        $context->{prov_subscriber}->{webpassword} = $subscriber->{web_password};
    #    }
    #
    #    unless (defined $webusername and length($webusername) > 0) {
    #        $webusername = $subscriber->{web_username};
    #    }
    #
    #    if (defined $subscriber->{barrings} and length($subscriber->{barrings}) > 0) {
    #        $barrings{$subscriber->{barrings}} = 1;
    #    }
    #
    #}
    #
    #unless (defined $context->{prov_subscriber}->{password} and length($context->{prov_subscriber}->{password}) > 0) {
    #    my $generated = _generate_sippassword($mta_sippassword_length);
    #    $context->{prov_subscriber}->{password} = $generated;
    #    _info($context,"empty sip_password, using generated '$generated'",1);
    #}
    #
    #unless (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0) {
    #    $context->{prov_subscriber}->{webusername} = $webusername;
    #    $context->{prov_subscriber}->{webpassword} = undef;
    #}
    #
    #if (not (defined $context->{prov_subscriber}->{webusername} and length($context->{prov_subscriber}->{webusername}) > 0)) {
    #    $context->{prov_subscriber}->{webusername} = undef;
    #    $context->{prov_subscriber}->{webpassword} = undef;
    #    _info($context,"empty web_username for sip_username '$first->{sip_username}'",1);
    #} else {
    #    $webusername = $context->{prov_subscriber}->{webusername};
    #    my %webusername_dupes = map { $_->{sip_username} => 1; }
    #        @{NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::findby_domain_webusername(
    #        $first->{domain},$webusername)};
    #    if ((scalar keys %webusername_dupes) > 1) {
    #        my $generated = _generate_webusername($mta_webusername_length); #$first->{sip_username};
    #        _info($context,"duplicate web_username '$webusername', using generated '$generated'",1);
    #        $context->{prov_subscriber}->{webusername} = $generated;
    #    }
    #
    #    #$context->{prov_subscriber}->{webpassword} = $first->{web_password};
    #    if (not (defined $context->{prov_subscriber}->{webpassword} and length($context->{prov_subscriber}->{webpassword}) > 0)) {
    #        my $generated = _generate_webpassword($mta_webpassword_length);
    #        _info($context,"empty web_password for web_username '$webusername', using generated '$generated'",1);
    #        $context->{prov_subscriber}->{webpassword} = $generated;
    #    #} elsif (defined $first->{web_password} and length($first->{web_password}) < 8) {
    #    #    $context->{prov_subscriber}->{webpassword} = _generate_webpassword();
    #    #    _info($context,"web_password for web_username '$first->{web_username}' is too short, using '$context->{prov_subscriber}->{webpassword}'");
    #    }
    #}
    #
    #$context->{ncos_level} = undef;
    #if ((scalar keys %barrings) > 1) {
    #    my $combined_barring = join('_',sort keys %barrings);
    #    #$result &=
    #    _check_ncos_level($context,$resellername,$combined_barring);
    #    _info($context,"barrings combination $combined_barring");
    #    $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$combined_barring};
    #} elsif ((scalar keys %barrings) == 1) {
    #    my ($barring) = keys %barrings;
    #    $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$barring};
    #} else {
    #    if (exists $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$default_barring}) {
    #        $context->{ncos_level} = $context->{ncos_level_map}->{$context->{reseller}->{id}}->{$default_barring};
    #        _info($context,"no ncos level, using default '$context->{ncos_level}->{level}'",1);
    #    }
    #}
    #
    #$context->{numbers} = {};
    #$context->{numbers}->{other} = sort_by_configs(\@numbers,[
    #    {   numeric     => 1,
    #        dir         => 1, #-1,
    #        memberchain => [ 'additional' ],
    #    },
    #    {   numeric     => 0,
    #        dir         => 1, #-1,
    #        memberchain => [ 'cc' ],
    #    },
    #    {   numeric     => 0,
    #        dir         => 1, #-1,
    #        memberchain => [ 'ac' ],
    #    },
    #    {   numeric     => 0,
    #        dir         => 1, #-1,
    #        memberchain => [ 'sn' ],
    #    },
    #]);
    #$context->{numbers}->{primary} = shift(@{$context->{numbers}->{other}});
    ##return 0 unless scalar @{$context->{numbers}->{other}};
    #
    #$context->{voip_numbers} = {};
    #$context->{voip_numbers}->{primary} = undef;
    #$context->{voip_numbers}->{other} = [];
    #$context->{aliases} = {};
    #$context->{aliases}->{primary} = undef;
    #$context->{aliases}->{other} = [];
    #
    #$context->{voicemail_user} = {};
    #$context->{voicemail_user}->{customer_id} = $context->{prov_subscriber}->{uuid};
    #$context->{voicemail_user}->{mailbox} = $context->{numbers}->{primary}->{number};
    #$context->{voicemail_user}->{password} = sprintf("%04d", int(rand 10000));
    #
    #$context->{preferences} = {};
    #
    #$context->{preferences}->{gpp} = [
    #    $first->{"_len"},
    #    $first->{"_cpe_mta_mac_address"},
    #    $first->{"_cpe_model"},
    #    $first->{"_cpe_vendor"},
    #];

    return $result;

}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    #push(@{$context->{log_error}},$message) if exists $context->{log_error};
    if ($context->{old_contract}) {
        $message = 'source contract id ' . $context->{old_contract}->{id} . ': ' . $message;
    }
    rowprocessingerror($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    #push(@{$context->{log_warning}},$message) if exists $context->{log_warning};
    if ($context->{old_contract}) {
        $message = 'source contract id ' . $context->{old_contract}->{id} . ': ' . $message;
    }
    rowprocessingwarn($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    #push(@{$context->{log_info}},$message) if exists $context->{log_info};
    if ($context->{old_contract}) {
        $message = 'source contract id ' . $context->{old_contract}->{id} . ': ' . $message;
    }
    if ($debug) {
        processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    }
}

1;
