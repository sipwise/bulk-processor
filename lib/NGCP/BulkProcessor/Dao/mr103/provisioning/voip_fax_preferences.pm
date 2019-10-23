package NGCP::BulkProcessor::Dao::mr103::provisioning::voip_fax_preferences;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

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

    source_findby_subscriberid
);

my $tablename = 'voip_fax_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'subscriber_id',
  'password',
  'name',
  'active',
  'send_status',
  'send_copy',

];

my $indexes = {};


sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

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

sub source_findby_subscriberid {

    my ($source_dbs,$subscriber_id) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs)->[0];

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...

            #delete $record->{subscriber_id};
            #delete $record->{id};

            push @records,$record;
        }
    }

    return \@records;

}

1;
