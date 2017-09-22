package NGCP::BulkProcessor::Projects::Migration::Teletek::Check;
use strict;

## no critic

no strict 'refs';

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::products qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Resellers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Domains qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    check_billing_db_tables
    check_provisioning_db_tables
    check_kamailio_db_tables
    check_import_db_tables
    check_rest_get_items
);

my $NOK = 'NOK';
my $OK = 'ok';

sub check_billing_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP billing db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::products');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::domains');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::contacts');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::contracts');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers');
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub check_import_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'import db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration');
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub check_provisioning_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP provisioning db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destination_sets');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations');
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub check_kamailio_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP kamailio db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users');
    $result &= $check_result; push(@$messages,$message);


    return $result;

}

sub _check_table {

    my ($message_prefix,$module) = @_;
    my $result = 0;
    my $message = ($message_prefix // '') . &{$module . '::gettablename'}() . ': ';
    eval {
        $result = &{$module . '::check_table'}();
    };
    if (@$ or not $result) {
        return (0,$message . $NOK);
    } else {
        return (1,$message . $OK);
    }

}

sub check_rest_get_items {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP id\'s/constants - ';

    return $result;
}


sub _check_rest_get_item {

    my ($message_prefix,$module,$id,$item_name_field,$get_method,$item_path_method) = @_;
    my $item = undef;
    $get_method //= 'get_item';
    $item_path_method //= 'get_item_path';
    my $message = ($message_prefix // '') . &{$module . '::' . $item_path_method}($id) . ': ';
    return (0,$message . $NOK,$item) unless $id;
    eval {
        $item = &{$module . '::' . $get_method}($id);
    };

    if (@$ or not defined $item or ('ARRAY' eq ref $item and (scalar @$item) != 1)) {
        return (0,$message . $NOK,$item);
    } else {
        $item = $item->[0] if ('ARRAY' eq ref $item and (scalar @$item) == 1);
        return (1,$message . "'" . $item->{$item_name_field} . "' " . $OK,$item);
    }

}

1;
