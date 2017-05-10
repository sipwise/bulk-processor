package NGCP::BulkProcessor::Dao::Trunk::billing::contacts;
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
    update_record
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

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

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
        my ($email,
            $reseller_id) = @params{qw/
                email
                reseller_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('create_timestamp') . ', ' .
                $db->columnidentifier('email') . ', ' .
                $db->columnidentifier('modify_timestamp') . ', ' .
                $db->columnidentifier('reseller_id') . ') VALUES (' .
                'NOW(), ' .
                '?, ' .
                'NOW(), ' .
                '?)',
                $email,
                $reseller_id,
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
