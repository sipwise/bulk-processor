package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_group;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
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

    findby_cdrid

);
#findby_callid

my $tablename = 'cdr_group';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
"cdr_id",
"call_id",
"cdr_start_time",
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

sub findby_cdrid {

    my ($xa_db,$cdrid,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('cdr_id') . ' = ?';
    my @params = ($cdrid);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

#sub findby_callid {
#
#    my ($callid,$load_recursive) = @_;
#
#    check_table();
#    my $db = &$get_db();
#    my $table = $db->tableidentifier($tablename);
#
#    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
#            $db->columnidentifier('call_id') . ' = ?';
#    my @params = ($callid);
#    my $rows = $db->db_get_all_arrayref($stmt,@params);
#
#    return buildrecords_fromrows($rows,$load_recursive);
#
#}

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
