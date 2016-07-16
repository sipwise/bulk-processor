package NGCP::BulkProcessor::Projects::Migration::IPGallery::Provisioning;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Subscribers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    destroy_all_dbs
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    test
);

    my $reseller_id = 1;
    my $billing_profile_id = 1;
    my $contact_email_format = '%s@melita.mt';

    my $domain_name = 'example.org';

sub test {

    my $result = 1;

    return $result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_subscriber (@$records) {
                $rownum++;
                next unless _reset_context($context,$imported_subscriber);

                eval {
                    $db->db_begin();

                    my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states(
                        $context->{billing_domain}->{id},$context->{username},{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE});
                    if ((scalar @$existing_billing_voip_subscribers) == 0) {

                        if ($imported_subscriber->{delta} eq
                            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {
                            warn...
                        } else {

                            my $existing_provisioning_voip_dbaliases = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_domainid_username(
                                $context->{provisioning_domain}->{id},$context->{cli});

                            if ((scalar @$existing_provisioning_voip_dbaliases) == 0) {
                                _create_contact($context);
                                _create_contract($context);




                            } else {
                                warn...
                                processing_info($context->{tid},(scalar @$existing_billing_voip_subscribers) . ' existing billing subscriber with username ' . $cli . ' found, skipping' ,getlogger(__PACKAGE__));
                            }
                        }
                    } elsif ((scalar @$existing_billing_voip_subscribers) == 1) {
                        if ($imported_subscriber->{delta} eq
                            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {
                            processing_info($context->{tid},'subscriber ' . $context->{cli} . ' deleted, skipping',getlogger(__PACKAGE__));
                            $success = NGCP::BulkProcessor::RestRequests::Trunk::Subscribers::delete_item($subscriber_id);

                            my $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::update_item($customer_id,{
                                status => "terminated",
                            });
                        } else {
                            processing_info($context->{tid},'existing billing subscriber with username ' . $context->{username} . ' found, skipping' ,getlogger(__PACKAGE__));
                            $context->{userpassworddelta}??
                            update passwords...
                        }
                    } else {
                        processing_info($context->{tid},(scalar @$existing_billing_voip_subscribers) . ' existing billing subscribers with username ' . $context->{username} . ' found, skipping' ,getlogger(__PACKAGE__));
                    }

                    if ($dry) {
                        $db->db_rollback();
                    } else {
                        $db->db_commit();
                    }

                };
                my $err = $@;
                if ($err) {
                    eval {
                        $db->db_rollback();
                    };
                    die($err) if !$skip_errors;
                }

            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{sip_account_product} = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
                $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];
            $context->{billing_domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_domain($domain_name);
            $context->{provisioning_voip_domain} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains::findby_domain($domain_name);
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        load_recursive => 0,
        multithreading => 1,
        numofthreads => 4,
    );
}

sub _create_contact {

    my ($context) = @_;

    $context->{contact_id} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($context->{db},
        reseller_id => $reseller_id,
        email => sprintf($contact_email_format,$context->{cli}),
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

sub _reset_context {

    my ($context,$imported_subscriber) = @_;

    my $result = 1;

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
        #delete $context->{username};
        #delete $context->{password};
        #delete $context->{userpassworddelta};
        #$result &= 0;

        # for now, as username+passwords are incomplete:
        $context->{username} = $context->{e164}->{sn};
        $context->{password} = $context->{username};
        $context->{userpassworddelta} = $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta;

        if ($skip_errors) {
            rowprocessingwarn($context->{tid},'no username/password for subscriber found: ' . $context->{cli},getlogger(__PACKAGE__));
        } else {
            rowprocessingerror($context->{tid},'no username/password for subscriber found: ' . $context->{cli},getlogger(__PACKAGE__));
        }
    }

    delete $context->{contact_id};
    delete $context->{contract_id};
    delete $context->{billing_mapping_id};
    delete $context->{contract_balance_id};

    return $result;

}

1;
