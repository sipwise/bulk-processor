package NGCP::BulkProcessor::Dao::mr38::billing::contacts;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findby_id
);

my $tablename = 'contacts';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'reseller_id',
    'gender',
    'firstname',
    'lastname',
    'comregnum',
    'company',
    'street',
    'postcode',
    'city',
    'country',
    'phonenumber',
    'mobilenumber',
    'email',
    'newsletter',
    'modify_timestamp',
    'create_timestamp',
    'faxnumber',
    'iban',
    'bic',
    'vatnum',
    'bankname',
    'gpp0',
    'gpp1',
    'gpp2',
    'gpp3',
    'gpp4',
    'gpp5',
    'gpp6',
    'gpp7',
    'gpp8',
    'gpp9',
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

sub gettablename {

    return $tablename;

}

sub check_table {

    return checktableinfo(shift // $get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

sub source_new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new_shared($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findby_id {

    my ($source_dbs,$id) = @_;

    my $source_db = $source_dbs->{billing_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs)->[0];

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{billing_db},$row);

            # transformations go here ...


            push @records,$record;
        }
    }

    return \@records;

}

1;
