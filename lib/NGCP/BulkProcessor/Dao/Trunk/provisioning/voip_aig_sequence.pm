package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowinsertskipped
    rowupdateskipped
    rowupdated
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_stmt
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    get_id
    increment
    cleanup_ids

);
#forupdate_increment

my $tablename = 'voip_aig_sequence';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
];

my $indexes = {};

my $insert_unique_fields = [];

#my $start_value = 1;
#my $increment = 1;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub get_id {

    my ($xa_db) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT ' . $db->columnidentifier('id') . ' FROM ' . $table . ' ORDER BY ID DESC LIMIT 1';
    return $xa_db->db_get_value($stmt);

}


sub increment {

    my ($xa_db) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = insert_stmt($db,__PACKAGE__);
    if ($xa_db->db_do($stmt,0)) {
        rowinserted($db,$tablename,getlogger(__PACKAGE__));
        return $xa_db->db_last_insert_id();
    } else {
        rowinsertskipped($db,$tablename,getlogger(__PACKAGE__));
        return undef;
    }

}

sub cleanup_ids {

    my ($xa_db) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    #You can't specify target table 'voip_aig_sequence' for update in FROM clause
    my $max_id = get_id($xa_db);
    if ($max_id) {
        my $stmt = 'DELETE FROM ' . $table . ' WHERE ' . $db->columnidentifier('id') . ' < ?';
        my $count;
        if ($count = $xa_db->db_do($stmt,$max_id)) {
            rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
            return 1;
        } else {
            rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
            return 0;
        }
    } else {
        return 0;
    }
}

#sub forupdate_increment {
#
#    my ($xa_db) = @_;
#
#    check_table();
#    my $db = &$get_db();
#    $xa_db //= $db;
#    my $table = $db->tableidentifier($tablename);
#
#    my $stmt = 'SELECT ' . $db->columnidentifier('id') . ' FROM ' . $table . ' ORDER BY ID DESC LIMIT 1 FOR UPDATE';
#    my $id = $xa_db->db_get_value($stmt);
#    if (defined $id) {
#        $stmt = 'UPDATE ' . $table . ' SET ' . $db->columnidentifier('id') . ' = ? WHERE ' . $db->columnidentifier('id') . ' = ?';
#        if ($xa_db->db_do($stmt,$id + $increment,$id)) {
#            rowupdated($db,$tablename,getlogger(__PACKAGE__));
#            return $id + $increment;
#        } else {
#            rowupdateskipped($db,$tablename,0,getlogger(__PACKAGE__));
#            return undef;
#        }
#    } else {
#        $stmt = insert_stmt($db,__PACKAGE__);
#        if ($xa_db->db_do($stmt,$start_value)) {
#            rowinserted($db,$tablename,getlogger(__PACKAGE__));
#            return $start_value;
#        } else {
#            rowinsertskipped($db,$tablename,getlogger(__PACKAGE__));
#            return undef;
#        }
#    }
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
