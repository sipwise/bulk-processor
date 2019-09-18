package NGCP::BulkProcessor::Dao::Trunk::billing::resellers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    findby_name
    findby_name_states
    findby_id
    findall

    insert_row

    $ACTIVE_STATE
    $TERMINATED_STATE
);

my $tablename = 'resellers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'name',
    'status',
];

my $indexes = {};

my $insert_unique_fields = []; #['name'];

our $ACTIVE_STATE = 'active';
our $TERMINATED_STATE = 'terminated';

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

sub findby_name {

    my ($name,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('name') . ' = ?';
    my @params = ($name);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findby_name_states {

    my ($xa_db,$name,$states,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    if (defined $name) {
        push(@terms,$db->columnidentifier('name') . ' = ?');
        push(@params,$name);
    }
    if (defined $states and 'HASH' eq ref $states) {
        foreach my $in (keys %$states) {
            my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
            push(@terms,$db->columnidentifier('status') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')');
            push(@params,@values);
        }
    } elsif (defined $states and length($states) > 0) {
        push(@terms,$db->columnidentifier('status') . ' = ?');
        push(@params,$states);
    }
    $stmt .= ' WHERE ' . join(' AND ',@terms) if (scalar @terms) > 0;
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_id {

    my ($id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

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
        my ($name,
            $contract_id) = @params{qw/
                name
                contract_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('name') . ', ' .
                $db->columnidentifier('status') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '\'' . $ACTIVE_STATE . '\')',
                $contract_id,
                $name,
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
