package NGCP::BulkProcessor::Dao::mr553::kamailio::location;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findby_username

);

my $tablename = 'location';
my $get_db = \&get_kamailio_db;

my $expected_fieldnames = [
  'id',
  'username',
  'domain',
  'contact',
  'received',
  'path',
  'expires',
  'q',
  'callid',
  'cseq',
  'last_modified',
  'flags',
  'cflags',
  'user_agent',
  'socket',
  'methods',
  'ruid',
  'reg_id',
  'instance',
  #'server_id',
  #'connection_id',
  #'keepalive',
  #'partition',

];

my $indexes = {};

my $insert_unique_fields = [];

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

    return checktableinfo($get_db,
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

sub source_findby_username_domain {

    my ($source_dbs,$username) = @_;

    my $source_db = $source_dbs->{kamailio_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('username') . ' = ?';
    my @params = ($username);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{kamailio_db},$row);

            # transformations go here ...

            push @records,$record;
        }
    }

    return \@records;

}

1;
