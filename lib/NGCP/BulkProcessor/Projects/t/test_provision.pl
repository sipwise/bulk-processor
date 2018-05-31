#!/usr/bin/perl
use warnings;
use strict;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingwarn
    fileprocessingerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    load_config
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
);

use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();

use NGCP::BulkProcessor::Utils qw(threadid);

load_config('config.cfg');

my $db = &get_xa_db();
{


    eval {
        $db->db_begin();

        my $exisiting_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states(
            $billing_domain->{id},$cli,{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE});
        if ((scalar @$exisiting_billing_voip_subscribers) == 0) {

            my $contact_id = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($db,
                reseller_id => $reseller_id,
                email => sprintf($contact_email_format,$cli),
            );
            #my $contact_id = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::insert_row($db,{
            #    reseller_id => $reseller_id,
            #    email => sprintf($contact_email_format,$cli),
            #});
            my $contract_id = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::insert_row($db,
                contact_id => $contact_id,
                #status => 'active',
            );

            my $billing_mapping_id = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::insert_row($db,
                billing_profile_id => $billing_profile_id,
                contract_id => $contract_id,
                product_id => $sip_account_product->{id},
            );

            my $contract_balance_id = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($db,
                contract_id => $contract_id,
            );

        } else {
            processing_info(threadid(),(scalar$exisiting_billing_voip_subscribers,getlogger(__PACKAGE__));
        }

        $db->db_commit();
    };
    my $err = $@;
    if ($err) {
        eval {
            $db->db_rollback();
        };
        die($err);
    }

    print "blah";
}
destroy_dbs();

exit;
