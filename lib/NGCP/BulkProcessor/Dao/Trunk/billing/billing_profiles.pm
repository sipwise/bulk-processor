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
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

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
