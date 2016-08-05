package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
    rowinserted
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

    insert_rows
    delete_groupid

    countby_groupid_ipnet

);

my $tablename = 'voip_allowed_ip_groups';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'group_id',
    'ipnet',
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

sub insert_rows {

    my ($xa_db,$group_id,$ipnets) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;

    my $stmt = insert_stmt($db,__PACKAGE__);

    my @ids = ();
    foreach my $ipnet (@$ipnets) {
        if ($xa_db->db_do($stmt,undef,$group_id,$ipnet)) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            push(@ids,$xa_db->db_last_insert_id());
        }
    }

    return \@ids;
}

sub delete_groupid {

    my ($xa_db,$group_id) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('group_id') . ' = ?';
    my @params = ($group_id);

    my $count;
    if ($count = $xa_db->db_do($stmt,@params)) {
        rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
        return 0;
    }

}

sub countby_groupid_ipnet {

    my ($group_id,$ipnet) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($group_id) {
        push(@terms,$db->columnidentifier('group_id') . ' = ?');
        push(@params,$group_id);
    }
    if ($ipnet) {
        push(@terms,$db->columnidentifier('ipnet') . ' = ?');
        push(@params,$ipnet);
    }

    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

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
