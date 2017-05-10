package NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles;
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

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    findby_id
    findby_resellerid_name_handle
);

my $tablename = 'billing_profiles';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'reseller_id',
    'handle',
    'name',
    'prepaid',
    'interval_charge',
    'interval_free_time',
    'interval_free_cash',
    'interval_unit',
    'interval_count',
    'fraud_interval_limit',
    'fraud_interval_lock',
    'fraud_interval_notify',
    'fraud_daily_limit',
    'fraud_daily_lock',
    'fraud_daily_notify',
    'fraud_use_reseller_rates',
    'currency',
    'status',
    'modify_timestamp',
    'create_timestamp',
    'terminate_timestamp',
];

my $indexes = {};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

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

sub findby_resellerid_name_handle {

    my ($reseller_id,$name,$handle,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($reseller_id) {
        push(@terms,$db->columnidentifier('reseller_id') . ' = ?');
        push(@params,$reseller_id);
    }
    if ($name) {
        push(@terms,$db->columnidentifier('name') . ' = ?');
        push(@params,$name);
    }
    if ($handle) {
        push(@terms,$db->columnidentifier('handle') . ' = ?');
        push(@params,$handle);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
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
