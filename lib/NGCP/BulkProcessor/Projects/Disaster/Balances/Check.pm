package NGCP::BulkProcessor::Projects::Disaster::Balances::Check;
use strict;

## no critic

no strict 'refs';

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();

#use NGCP::BulkProcessor::RestRequests::Trunk::Contracts qw();
#use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();
#use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    check_billing_db_tables
);
#check_rest_get_items

my $NOK = 'NOK';
my $OK = 'ok';

sub check_billing_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP billing db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::contracts');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances');
    $result &= $check_result; push(@$messages,$message);

    #($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers');
    #if (not $check_result) {
    #    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers');
    #}
    #$result &= $check_result; push(@$messages,$message);

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

#sub check_rest_get_items {
#
#    my ($messages) = @_;
#
#    my $result = 1;
#    my $check_result;
#    my $message;
#
#    my $message_prefix = 'NGCP id\'s/constants - ';
#
#    ($check_result,$message, my $reseller) = _check_rest_get_item($message_prefix,
#        'NGCP::BulkProcessor::RestRequests::Trunk::Resellers',
#        $reseller_id,
#        'name');
#    $result &= $check_result; push(@$messages,$message);
#
#
#    return $result;
#
#}

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
