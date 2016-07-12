package NGCP::BulkProcessor::Projects::Migration::IPGallery::Check;
use strict;

## no critic

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    check_billing_db_tables
    check_import_db_tables
);

my $NOK = 'NOK';
my $OK = 'ok';

sub check_billing_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP billing db tables - ';

    ($check_result,$message) = _check_billing_contracts_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_billing_contract_balances_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_billing_billing_mappings_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub _check_billing_contracts_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Dao::Trunk::billing::contracts::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Dao::Trunk::billing::contracts::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_billing_contract_balances_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_billing_billing_mappings_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}


sub check_import_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'import db tables - ';

    ($check_result,$message) = _check_import_feature_option_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_import_feature_option_set_item_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_import_user_password_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_import_subscriber_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_import_batch_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_import_lnp_table($message_prefix);
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub _check_import_subscriber_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_import_feature_option_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_import_feature_option_set_item_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_import_lnp_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_import_user_password_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub _check_import_batch_table {

    my ($message_prefix) = @_;
    my $result = 1;
    eval {
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::check_table();
    };
    my $message = ($message_prefix // '') . NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::gettablename() . ': ';
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

1;
