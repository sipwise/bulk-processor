package NGCP::BulkProcessor::Projects::Disaster::Acc::Check;
use strict;

## no critic

no strict 'refs';

use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();
use NGCP::BulkProcessor::Dao::Trunk::kamailio::acc qw();
use NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    check_accounting_db_tables
    check_kamailio_db_tables
);
#check_rest_get_items

my $NOK = 'NOK';
my $OK = 'ok';

sub check_kamailio_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP kamailio db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::kamailio::acc');
    $result &= $check_result; push(@$messages,$message);

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash');
    $result &= $check_result; push(@$messages,$message);

    return $result;

}

sub check_accounting_db_tables {

    my ($messages) = @_;

    my $result = 1;
    my $check_result;
    my $message;

    my $message_prefix = 'NGCP accounting db tables - ';

    ($check_result,$message) = _check_table($message_prefix,'NGCP::BulkProcessor::Dao::Trunk::accounting::cdr');
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
#        'NGCP::BulkProcessor::RestRequests::mr38::Resellers',
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
