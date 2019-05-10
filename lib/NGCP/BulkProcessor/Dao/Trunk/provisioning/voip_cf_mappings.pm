package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings;
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
    copy_row
    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    countby_subscriberid_type
    $CFB_TYPE
    $CFT_TYPE
    $CFU_TYPE
    $CFNA_TYPE

    insert_row
    delete_cfmappings
);

my $tablename = 'voip_cf_mappings';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'subscriber_id',
    'type',
    'destination_set_id',
    'time_set_id',
];

my $indexes = {};

my $insert_unique_fields = [];

our $CFB_TYPE = 'cfb';
our $CFT_TYPE = 'cft';
our $CFU_TYPE = 'cfu';
our $CFNA_TYPE = 'cfna';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub countby_subscriberid_type {

    my ($subscriber_id,$type,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($subscriber_id) {
        push(@terms,$db->columnidentifier('subscriber_id') . ' = ?');
        push(@params,$subscriber_id);
    }
    if ($type) {
        push(@terms,$db->columnidentifier('type') . ' = ?');
        push(@params,$type);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub delete_cfmappings {

    my ($xa_db,$subscriber_id,$types) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $types and 'HASH' eq ref $types) {
        foreach my $in (keys %$types) {
            my @values = (defined $types->{$in} and 'ARRAY' eq ref $types->{$in} ? @{$types->{$in}} : ($types->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('type') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
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
        if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($subscriber_id,
            $type,
            $destination_set_id,
            $time_set_id) = @params{qw/
                subscriber_id
                type
                destination_set_id
                time_set_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('subscriber_id') . ', ' .
                $db->columnidentifier('type') . ', ' .
                $db->columnidentifier('destination_set_id') . ', ' .
                $db->columnidentifier('time_set_id') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?)',
                $subscriber_id,
                $type,
                $destination_set_id,
                $time_set_id
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
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
