package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
    rowinserted
    rowupserted
    rowupdated
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    copy_row

);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    update_row
    insert_row
    upsert_row

    $UNEXPORTED
    $EXPORTED
);

our $UNEXPORTED = 'unexported';
our $EXPORTED = 'exported';

my $tablename = 'cdr_export_status_data';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
"cdr_id",
"status_id",
"exported_at",
"export_status",
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

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

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
        my ($cdr_id,
            $status_id,
            $export_status,
            $cdr_start_time) = @params{qw/
                cdr_id
                status_id
                export_status
                cdr_start_time
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('cdr_id') . ', ' .
                $db->columnidentifier('status_id') . ', ' .
                $db->columnidentifier('export_status') . ', ' .
                $db->columnidentifier('exported_at') . ', ' .
                $db->columnidentifier('cdr_start_time') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                'NOW(), ' .
                '?)',
                $cdr_id,
                $status_id,
                $export_status,
                $cdr_start_time,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return 1;
        }
    }
    return undef;

}

sub upsert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;

    my %params = @_;
    my ($cdr_id,
        $status_id,
        $export_status,
        $cdr_start_time) = @params{qw/
            cdr_id
            status_id
            export_status
            cdr_start_time
        /};

    if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
            $db->columnidentifier('cdr_id') . ', ' .
            $db->columnidentifier('status_id') . ', ' .
            $db->columnidentifier('export_status') . ', ' .
            $db->columnidentifier('exported_at') . ', ' .
            $db->columnidentifier('cdr_start_time') . ') VALUES (' .
            '?, ' .
            '?, ' .
            '?, ' .
            'NOW(), ' .
            '?) ON DUPLICATE KEY UPDATE export_status = ?, exported_at = NOW()',
            $cdr_id,
            $status_id,
            $export_status,
            $cdr_start_time,
            $export_status,
        )) {
        rowupserted($db,$tablename,getlogger(__PACKAGE__));
        return 1;
    }

    return undef;

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
