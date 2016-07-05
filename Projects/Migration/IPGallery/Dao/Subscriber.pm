package Projects::Migration::IPGallery::Dao::Subscriber;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

#use Logging qw(getlogger);

use Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db

);
#import_db_tableidentifier

use SqlRecord qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
);

require Exporter;
our @ISA = qw(Exporter SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement

    test_table_bycolumn1
    test_table_local_select
    test_table_source_select
    test_table_source_select_temptable
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

my $primarykey_fieldnames = [ 'country_code', 'area_code', 'dial_number' ];

my $indexes = {};

my $fixtable_statements = [];

sub new {

    my $class = shift;
    my $self = SqlRecord->new($get_db,
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

    check_table();
    return insert_stmt($get_db,$tablename);

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
