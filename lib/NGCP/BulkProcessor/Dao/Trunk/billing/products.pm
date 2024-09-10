package NGCP::BulkProcessor::Dao::Trunk::billing::products;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Array qw(array_to_map); 

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    findby_id
    findall
    findby_id_cached
    findby_resellerid_handle

    $PSTN_PEERING_ACCOUNT_HANDLE
    $SIP_PEERING_ACCOUNT_HANDLE
    $VOIP_RESELLER_ACCOUNT_HANDLE
    $SIP_ACCOUNT_HANDLE
    $PBX_ACCOUNT_HANDLE
);

my $tablename = 'products';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'reseller_id',
    'class',
    'handle',
    'name',
    'on_sale',
    'price',
    'weight',
    'billing_profile_id',
];

my $indexes = {};

our $PSTN_PEERING_ACCOUNT_HANDLE = 'PSTN_PEERING';
our $SIP_PEERING_ACCOUNT_HANDLE = 'SIP_PEERING';
our $VOIP_RESELLER_ACCOUNT_HANDLE = 'VOIP_RESELLER';
our $SIP_ACCOUNT_HANDLE = 'SIP_ACCOUNT';
our $PBX_ACCOUNT_HANDLE = 'PBX_ACCOUNT';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findall {

    my ($load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

my $product_map;

sub findby_id_cached {
    my ($id,$load_recursive) = @_;
    unless ($product_map) {
        ($product_map, my $types, my $ids) = array_to_map(findall($load_recursive),
            sub { return shift->{id}; }, sub { return shift; }, 'last');
    }
    return __PACKAGE__->new($product_map->{$id}) if defined $id;
    return;
}

sub findby_id {

    my ($id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findby_resellerid_handle {

    my ($reseller_id,$handle,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('reseller_id') . ' <=> ?';
    my @params = ($reseller_id);
    if (defined $handle) {
        $stmt .= ' AND ' . $db->columnidentifier('handle') . ' = ?';
        push(@params,$handle);
    }
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...

            push @records,$record;
        }
    }

    return \@records;

}

sub gettablename {

    return $tablename;

}

sub check_table {

    return checktableinfo($get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
