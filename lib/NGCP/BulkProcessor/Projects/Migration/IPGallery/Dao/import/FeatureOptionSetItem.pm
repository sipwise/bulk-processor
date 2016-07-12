package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem;
use strict;

## no critic

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
    getupsertstatement

    findby_subscribernumber_option_optionsetitem
    countby_subscribernumber_option_optionsetitem

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta

    $PRE_PAID_SERVICE_OPTION_SET
);

my $tablename = 'feature_option_set_item';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [
    'subscribernumber',
    'option',
    'optionsetitem',
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'subscribernumber', 'option', 'optionsetitem' ];
my $indexes = { $tablename . '_delta' => [ 'delta(7)' ]};
#    $tablename . '_subscribernumber_option_optionsetitem' => ['subscribernumber(11)', 'option(32)', 'optionsetitem(32)'], #(25),(27)
#};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

our $PRE_PAID_SERVICE_OPTION_SET = 'Pre_Paid_Service';

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

sub findby_subscribernumber_option_optionsetitem {

    my ($subscribernumber,$option,$optionsetitem,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table .
        ' WHERE ' . $db->columnidentifier('subscribernumber') . ' = ? ' .
        ' AND ' . $db->columnidentifier('option') . ' = ?';
    my @params = ($subscribernumber,$option);
    if (defined $optionsetitem and length($optionsetitem) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('optionsetitem') . ' = ?';
        push(@params,$optionsetitem);
    }

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub update_delta {

    my ($subscribernumber,$option,$optionsetitem,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $subscribernumber) {
        $stmt .= ' WHERE ' . $db->columnidentifier('subscribernumber') . ' = ?';
        push(@params,$subscribernumber);
        if (defined $option) {
            $stmt .= ' AND ' . $db->columnidentifier('option') . ' = ?';
            push(@params,$option);
            if (defined $optionsetitem and length($optionsetitem) > 0) {
                $stmt .= ' AND ' . $db->columnidentifier('optionsetitem') . ' = ?';
                push(@params,$optionsetitem);
            }
        }
    }

    return $db->db_do($stmt,@params);

}

sub countby_subscribernumber_option_optionsetitem {

    my ($subscribernumber,$option,$optionsetitem) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $subscribernumber) {
        $stmt .= ' WHERE ' . $db->columnidentifier('subscribernumber') . ' = ?';
        push(@params,$subscribernumber);
        if (defined $option) {
            $stmt .= ' AND ' . $db->columnidentifier('option') . ' = ?';
            push(@params,$option);
            if (defined $optionsetitem and length($optionsetitem) > 0) {
                $stmt .= ' AND ' . $db->columnidentifier('optionsetitem') . ' = ?';
                push(@params,$optionsetitem);
            }
        }
    }

    return $db->db_get_value($stmt,@params);

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

sub getupsertstatement {

    my ($exists_delta,$new_delta) = @_;
    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);
    my $upsert_stmt = 'INSERT OR REPLACE INTO ' . $table . ' (' .
      join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @$expected_fieldnames) . ')';
    my @values = ();
    foreach my $fieldname (@$expected_fieldnames) {
        if ('delta' eq $fieldname) {
            my $stmt = 'SELECT \'' . $exists_delta . '\' FROM ' . $table . ' WHERE ' .
               $db->columnidentifier('subscribernumber') . ' = ?' .
               ' AND ' . $db->columnidentifier('option') . ' = ?' .
               ' AND ' . $db->columnidentifier('optionsetitem') . ' = ?';
            push(@values,'COALESCE((' . $stmt . '), \'' . $new_delta . '\')');
        } else {
            push(@values,'?');
        }
    }
    $upsert_stmt .= ' VALUES (' . join(',',@values) . ')';
    return $upsert_stmt;

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
