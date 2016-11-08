package NGCP::BulkProcessor::Projects::Migration::IPGallery::Check;
use strict;

## no critic

no strict 'refs';



use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $reseller_id
    $domain_name
    $billing_profile_id
    $barring_profiles
);

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
use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_domains qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::RestRequests::Trunk::Resellers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Domains qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();
use NGCP::BulkProcessor::RestRequests::Trunk::NcosLevels qw();
use NGCP::BulkProcessor::RestRequests::Trunk::LnpCarriers qw();

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

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers');
    if (not $check_result) {
        ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers');
    }
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub check_import_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'import db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp');
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

    ($check_result,$message, my $reseller) = _check_rest_get_item($message_prefix,
        'NGCP::BulkProcessor::RestRequests::Trunk::Resellers',
        $reseller_id,
        'name');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message, my $domain) = _check_rest_get_item($message_prefix,
        'NGCP::BulkProcessor::RestRequests::Trunk::Domains',
        { 'domain' => $domain_name, 'reseller_id' => $reseller_id },
        'domain',
        'get_item_filtered',
        'get_item_filter_path');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message, my $domain) = _check_rest_get_item($message_prefix,
        'NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles',
        $billing_profile_id,
        'handle');
    $result &= $check_result; push(@$messages,$message);

    foreach my $level (values %$barring_profiles) {
        if (defined $level and length($level) > 0) {
            ($check_result,$message, my $ncos_level) = _check_rest_get_item($message_prefix,
                'NGCP::BulkProcessor::RestRequests::Trunk::NcosLevels',
                { 'level' => $level, 'reseller_id' => $reseller_id },
                'level',
                'get_item_filtered',
                'get_item_filter_path');
            $result &= $check_result; push(@$messages,$message);
        }
    }

    my $lrn_codes = [];
    eval {
        $lrn_codes = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::list_lrncodes(
        #{
        #    'NOT IN' => $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta
        #}
        );
    };
    foreach my $prefix (@$lrn_codes) {
        ($check_result,$message, my $lnp_carrier) = _check_rest_get_item($message_prefix,
            'NGCP::BulkProcessor::RestRequests::Trunk::LnpCarriers',
            { 'prefix' => $prefix, },
            'name',
            'get_items_filtered',
            'get_item_filter_path');
        $result &= $check_result; push(@$messages,$message);
    }

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
