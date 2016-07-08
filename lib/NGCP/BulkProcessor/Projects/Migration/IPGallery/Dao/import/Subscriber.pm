package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber;
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

    findby_subscribernumber
    countby_subscribernumber
);

my $tablename = 'subscriber';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [
    'country_code', #356
    'area_code', #None
    'dial_number', #35627883323
    'rgw_fqdn', #35627883323
    'port', #None
    'region_name', #None
    'carrier_code', #None
    'time_zone_name', #malta
    'lang_code', #eng
    'barring_profile', #None
];

# table creation:
my $primarykey_fieldnames = [ 'country_code', 'area_code', 'dial_number' ];
my $indexes = {};
#my $fixtable_statements = [];

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

    return [] unless defined $subscribernumber;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('country_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('dial_number') . ' = ?'
    ,split_subscribernumber($subscribernumber));

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
        $stmt .= ' WHERE ' .
            $db->columnidentifier('country_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('dial_number') . ' = ?';
        push(@params,split_subscribernumber($subscribernumber));
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
                $record->{_features} = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::findby_subscribernumber(
                    $record->subscribernumber(),
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

sub subscribernumber {
    my $self = shift;
    return $self->{dial_number}; #$self->{country_code} . $self->{dial_number};
}

sub split_subscribernumber {
    my ($subscribernumber) = @_;
    my $country_code = substr($subscribernumber,0,3);
    my $area_code = 'None';
    my $dial_number = $subscribernumber; #substr($subscribernumber,3);
    return ($country_code,$area_code,$dial_number);
}

1;
