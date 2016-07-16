#!/usr/bin/perl
use warnings;
use strict;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

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

load_config('config.cfg');

my $db = &get_xa_db();
{
    my $reseller_id = 1;
    my $billing_profile_id = 1;
    my $contact_email_format = '%s@melita.mt';
    my $sip_account_product = NGCP::BulkProcessor::Dao::Trunk::billing::products::findby_resellerid_handle(undef,
        $NGCP::BulkProcessor::Dao::Trunk::billing::products::SIP_ACCOUNT_HANDLE)->[0];


    my $subscribernumber = { cc => '43', ac => '', sn => '12345678' };
    my $cli = $subscribernumber->{cc} . $subscribernumber->{ac} . $subscribernumber->{sn};

    eval {
        $db->db_begin();
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
            status => 'active',
        );

        my $billing_mapping_id = NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::insert_row($db,
            billing_profile_id => $billing_profile_id,
            contract_id => $contract_id,
            product_id => $sip_account_product->{id},
        );

        my $contract_balance_id = NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::insert_row($db,
            contract_id => $contract_id,
        );



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
