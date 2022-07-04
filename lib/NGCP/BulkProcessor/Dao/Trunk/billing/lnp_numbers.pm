package NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    delete_record
    copy_row
    insert_stmt
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
    
    delete_numbers
    
    getinsertstatement

    findby_lnpproviderid_number
    countby_lnpproviderid_number
    
    @fieldnames
);

my $tablename = 'lnp_numbers';
my $get_db = \&get_billing_db;

our @fieldnames = (
    'id',
    'number',
    'routing_number',
    'lnp_provider_id',
    'start',
    'end',
);

my $expected_fieldnames = [
    @fieldnames
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

sub findby_lnpproviderid_number {

    my ($xa_db,$lnp_provider_id,$number,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('number') . ' = ?';
    my @params = ($number);
    if (defined $lnp_provider_id) {
        $stmt .= ' AND ' . $db->columnidentifier('lnp_provider_id') . ' = ?';
        push(@params,$lnp_provider_id);
    }

    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_lnpproviderid_number {

    my ($lnp_provider_id,$number,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @terms = ();
    my @params = ();
    if (defined $lnp_provider_id) {
        push(@terms,$db->columnidentifier('lnp_provider_id') . ' = ?');
        push(@params,$lnp_provider_id);
    }
    if (defined $number) {
        push(@terms,$db->columnidentifier('number') . ' = ?');
        push(@params,$number);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub delete_row {

    my ($xa_db,$data) = @_;

    check_table();
    return delete_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub delete_numbers {

    my ($xa_db,$numbers) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = '';
    my @params = ();
    if (defined $numbers and 'HASH' eq ref $numbers) {
        foreach my $in (keys %$numbers) {
            my @values = (defined $numbers->{$in} and 'ARRAY' eq ref $numbers->{$in} ? @{$numbers->{$in}} : ($numbers->{$in}));
            $stmt .= ' AND ' if length($stmt);
            $stmt .= $db->columnidentifier('number') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $numbers and length($numbers) > 0) {
        $stmt = $db->columnidentifier('number') . ' = ?';
        push(@params,$numbers);
    }
    
    $stmt = ' WHERE ' . $stmt if length($stmt);
    $stmt = 'DELETE FROM ' . $table . $stmt;

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
        my ($number,
            $lnp_provider_id) = @params{qw/
                number
                lnp_provider_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('number') . ', ' .
                $db->columnidentifier('lnp_provider_id') . ') VALUES (' .
                '?, ' .
                '?)',
                $number,
                $lnp_provider_id,
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

sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,__PACKAGE__,$insert_ignore);

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
