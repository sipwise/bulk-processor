package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    delete_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row
    delete_row

    delete_preferences

    findby_subscriberid_attributeid
);

my $tablename = 'voip_usr_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'subscriber_id',
    'attribute_id',
    'value',
    'modify_timestamp',
];

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_subscriberid_attributeid {

    my ($xa_db,$subscriber_id,$attribute_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $attribute_id) {
        $stmt .= ' AND ' . $db->columnidentifier('attribute_id') . ' = ?';
        push(@params,$attribute_id);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,$tablename,$data);

}

sub delete_row {

    my ($xa_db,$data) = @_;

    check_table();
    return delete_record($get_db,$xa_db,$tablename,$data);

}

sub delete_preferences {

    my ($xa_db,$subscriber_id,$attribute_id,$vals) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $attribute_id) {
        $stmt .= ' AND ' . $db->columnidentifier('attribute_id') . ' = ?';
        push(@params,$attribute_id);
    }
    if (defined $vals and 'HASH' eq ref $vals) {
        foreach my $in (keys %$vals) {
            my @values = (defined $vals->{$in} and 'ARRAY' eq ref $vals->{$in} ? @{$vals->{$in}} : ($vals->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('id') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    }

    my $count;
    if ($count = $xa_db->db_do($stmt,@params)) {
        rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
        return 0;
    }

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,$tablename,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($attribute_id,
            $subscriber_id,
            $value) = @params{qw/
                attribute_id
                subscriber_id
                value
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('attribute_id') . ', ' .
                $db->columnidentifier('subscriber_id') . ', ' .
                $db->columnidentifier('value') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?)',
                $attribute_id,
                $subscriber_id,
                $value,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
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
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
