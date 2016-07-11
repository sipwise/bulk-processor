package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db

);
#import_db_tableidentifier

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement

    findby_lrncode_portednumber
    countby_lrncode_portednumber
    count_lrncodes

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta
);

my $tablename = 'lnp';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [
    'ported_number',
    'type',
    'lrn_code',
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'lrn_code', 'ported_number' ];
my $indexes = { $tablename . '_delta' => [ 'delta(7)' ]};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub create_table {

    my ($truncate) = @_;

    my $db = &$get_db();

    registertableinfo($db,$tablename,$expected_fieldnames,$indexes,$primarykey_fieldnames);
    return create_targettable($db,$tablename,$db,$tablename,$truncate,0,undef);

}

sub findby_delta {

    my ($delta,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless defined $delta;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('delta') . ' = ?'
    ,$delta);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_lrncode_portednumber {

    my ($lrncode,$portednumber,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('lrn_code') . ' = ? ' .
            ' AND ' . $db->columnidentifier('ported_number') . ' = ?'
    ,$lrncode,$portednumber);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub update_delta {

    my ($lrncode,$portednumber,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $lrncode) {
        $stmt .= ' WHERE ' . $db->columnidentifier('lrn_code') . ' = ?';
        push(@params,$lrncode);
        if (defined $portednumber) {
            $stmt .= ' AND ' . $db->columnidentifier('ported_number') . ' = ?';
            push(@params,$portednumber);
        }
    }

    return $db->db_do($stmt,@params);

}

sub countby_lrncode_portednumber {

    my ($lrncode,$portednumber) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $lrncode) {
        $stmt .= ' WHERE ' . $db->columnidentifier('lrn_code') . ' = ?';
        push(@params,$lrncode);
        if (defined $portednumber) {
            $stmt .= ' AND ' . $db->columnidentifier('ported_number') . ' = ?';
            push(@params,$portednumber);
        }
    }

    return $db->db_get_value($stmt,@params);

}

sub count_lrncodes {

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return $db->db_get_value('SELECT COUNT(DISTINCT ' .
        $db->columnidentifier('lrn_code') . ') FROM ' . $table);

}

sub countby_delta {

    my ($delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $delta) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('delta') . ' = ?';
        push(@params,$delta);
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

sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,$tablename,$insert_ignore);

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
