package NGCP::BulkProcessor::Dao::mr553::provisioning::voip_cf_periods;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db

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

    source_findby_timesetid

);

my $tablename = 'voip_cf_periods';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'time_set_id',
  'year',
  'month',
  'mday',
  'wday',
  'hour',
  'minute',
];

my $indexes = {};


sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

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

    return checktableinfo(shift // $get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

sub source_new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new_shared($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findby_timesetid {

    my ($source_dbs,$time_set_id) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('time_set_id') . ' = ?';
    my @params = ($time_set_id);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...

            push @records,$record;
        }
    }

    return \@records;

}

1;
