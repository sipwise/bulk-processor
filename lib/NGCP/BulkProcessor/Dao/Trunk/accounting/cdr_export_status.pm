package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status;
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
    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Array qw(array_to_map); 

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    insert_row

    findall
    findby_type
    findby_id
    findby_id_cached
);

my $tablename = 'cdr_export_status';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
"id",
"type",
];

my $indexes = {};

my $insert_unique_fields = [ 'type' ];

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
    my $rows = $db->db_get_all_arrayref($stmt);

    return buildrecords_fromrows($rows,$load_recursive);

}

my $cdr_export_status_map;

sub findby_id_cached {
    my ($id,$load_recursive) = @_;
    unless ($cdr_export_status_map) {
        ($cdr_export_status_map, my $streams, my $ids) = array_to_map(findall($load_recursive),
            sub { return shift->{id}; }, sub { return shift; }, 'last');
    }
    return __PACKAGE__->new($cdr_export_status_map->{$id}) if defined $id;
    return;
}

sub findby_id {

    my ($xa_db,$id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_type {

    my ($type,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('type') . ' = ?';
    my @params = ($type);
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

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($type) = @params{qw/
                type
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('type') . ') VALUES (' .
                '?)',
                $type,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

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
