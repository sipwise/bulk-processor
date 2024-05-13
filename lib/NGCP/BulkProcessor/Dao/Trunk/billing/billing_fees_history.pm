package NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
    destroy_dbs
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

my $tablename = 'billing_fees_history';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
  'id',
  'bf_id',
  'billing_profile_id',
  'billing_zones_history_id',
  'source',
  'destination',
  'direction',
  'type',
  'onpeak_init_rate',
  'onpeak_init_interval',
  'onpeak_follow_rate',
  'onpeak_follow_interval',
  'offpeak_init_rate',
  'offpeak_init_interval',
  'offpeak_follow_rate',
  'offpeak_follow_interval',
  'onpeak_use_free_time',
  'match_mode',
  'onpeak_extra_rate',
  'onpeak_extra_second',
  'offpeak_extra_rate',
  'offpeak_extra_second',
  'offpeak_use_free_time',
  'aoc_pulse_amount_per_message',
];

my $indexes = {};

my $insert_unique_fields = [];

#enum('regex_longest_pattern','regex_longest_match','prefix','exact_destination')

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
