package NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages;
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
    findall

    $CARRY_OVER_MODE
    $CARRY_OVER_TIMELY_MODE
    $DISCARD_MODE
    $DEFAULT_CARRY_OVER_MODE
    $DEFAULT_INITIAL_BALANCE
);

my $tablename = 'profile_packages';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
  'id',
  'reseller_id',
  'name',
  'description',
  'initial_balance',
  'service_charge',
  'balance_interval_unit',
  'balance_interval_value',
  'balance_interval_start_mode',
  'carry_over_mode',
  'timely_duration_unit',
  'timely_duration_value',
  'notopup_discard_intervals',
  'underrun_lock_threshold',
  'underrun_lock_level',
  'underrun_profile_threshold',
  'topup_lock_level',
];

my $indexes = {};

our $CARRY_OVER_MODE = 'carry_over';
our $CARRY_OVER_TIMELY_MODE = 'carry_over_timely';
our $DISCARD_MODE = 'discard';
our $DEFAULT_CARRY_OVER_MODE = $CARRY_OVER_MODE;
our $DEFAULT_INITIAL_BALANCE = 0.0;

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

sub findall {

    my ($load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my $rows = $db->db_get_all_arrayref($stmt);

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
