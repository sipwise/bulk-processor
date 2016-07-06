package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOption;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db

);
#import_db_tableidentifier

use NGCP::BulkProcessor::SqlRecord qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSetItem qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement

    findby_subscribernumber
    countby_subscribernumber
);

my $tablename = 'feature_option';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [ 'subscribernumber',
                            'option'];

my $primarykey_fieldnames = [ 'subscribernumber', 'option' ];

my $indexes = {};

my $fixtable_statements = [];

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

sub findby_subscribernumber {

    my ($subscribernumber,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('subscribernumber') . ' = ?'
    ,$subscribernumber);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_subscribernumber {

    my ($subscribernumber) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $subscribernumber) {
        $stmt .= ' WHERE ' . $db->columnidentifier('subscribernumber') . ' = ?';
        push(@params,$subscribernumber);
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
            if ($load_recursive) {
                $record->{_optionsetitems} = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSetItem::findby_subscribernumber_option(
                    $record->{subscribernumber},
                    $record->{option},
                    $load_recursive
                );
            }

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
