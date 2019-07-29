package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_tag;
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

    findall

    $CALLING_PARTY_CATEGORY
    $FURNISHED_CHARGING_INFO
    $HEADER_DIVERSION
    $HEADER_P_ASSERTED_IDENTITY
    $HEADER_USER_TO_USER

);


my $tablename = 'cdr_tag';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
  "id",
  "type",
];

our $CALLING_PARTY_CATEGORY = 'calling_party_category';
our $FURNISHED_CHARGING_INFO = 'furnished_charging_info';
our $HEADER_DIVERSION = 'header=Diversion';
our $HEADER_P_ASSERTED_IDENTITY = 'header=P-Asserted-Identity';
our $HEADER_USER_TO_USER = 'header=User-to-User';

my $indexes = {};

my $insert_unique_fields = [];

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
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

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
