#!/usr/bin/perl
use warnings;
use strict;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use NGCP::BulkProcessor::LoadConfig qw(
    load_config
);

use NGCP::BulkProcessor::RestRequests::Trunk::SystemContacts qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Contracts qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Resellers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingZones qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingFees qw();

load_config('config.cfg');

{
    my $t = time();
    my $systemcontact_id = NGCP::BulkProcessor::RestRequests::Trunk::SystemContacts::create_item({
        email => "test$t\@blah.com",
    });
    my $contract_id = NGCP::BulkProcessor::RestRequests::Trunk::Contracts::create_item({
   		contact_id => $systemcontact_id,
		billing_profile_id => 1, #default profile id
		type => 'reseller',
        status => 'active',
    });
    my $reseller_id = NGCP::BulkProcessor::RestRequests::Trunk::Resellers::create_item({
        contract_id => $contract_id,
        name => "reseller$t",
        status => 'active',
    });

    my $billing_profile_id = NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles::create_item({
        handle => "profile$t",
        name => "profile$t",
        reseller_id => $reseller_id,
    });

    my $billing_zone_id = NGCP::BulkProcessor::RestRequests::Trunk::BillingZones::create_item({
        billing_profile_id => $billing_profile_id,
        zone => "zone$t",
    });

    my $billing_fee_id = NGCP::BulkProcessor::RestRequests::Trunk::BillingFees::create_item({
        billing_profile_id => $billing_profile_id,
        billing_zone_id => $billing_zone_id,
                direction => 'out',
                destination => '.*',
                onpeak_init_rate        => 10,
                onpeak_init_interval    => 1,
                onpeak_follow_rate      => 5,
                onpeak_follow_interval  => 2,
                offpeak_init_rate        => 8,
                offpeak_init_interval    => 1,
                offpeak_follow_rate      => 4,
                offpeak_follow_interval  => 2,
    });

    my $success = NGCP::BulkProcessor::RestRequests::Trunk::BillingFees::delete_item($billing_fee_id);

    my $contract = NGCP::BulkProcessor::RestRequests::Trunk::Contracts::set_item($contract_id,{
   		contact_id => $systemcontact_id,
		billing_profile_id => $billing_profile_id,
		type => 'reseller',
        status => 'active',
    });

    $contract = NGCP::BulkProcessor::RestRequests::Trunk::Contracts::update_item($contract_id,{
   		contact_id => $systemcontact_id,
		billing_profile_id => $billing_profile_id,
		#type => 'reseller',
        status => 'active',
    });

}

exit;
