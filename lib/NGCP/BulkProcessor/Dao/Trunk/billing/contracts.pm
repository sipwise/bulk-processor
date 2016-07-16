package NGCP::BulkProcessor::Dao::Trunk::billing::contracts;
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
    insert_row
);

my $tablename = 'contracts';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'customer_id',
    'contact_id',
    'order_id',
    'profile_package_id',
    'status',
    'external_id',
    'modify_timestamp',
    'create_timestamp',
    'activate_timestamp',
    'terminate_timestamp',
    'max_subscribers',
    'send_invoice',
    'subscriber_email_template_id',
    'passreset_email_template_id',
    'invoice_email_template_id',
    'invoice_template_id',
    'vat_rate',
    'add_vat',
];

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,$tablename,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($contact_id) = @params{qw/
                contact_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('contact_id') . ', ' .
                $db->columnidentifier('create_timestamp') . ', ' .
                $db->columnidentifier('modify_timestamp') . ', ' .
                $db->columnidentifier('status') . ') VALUES (' .
                '?, ' .
                'NOW(), ' .
                'NOW(), ' .
                '\'active\')',
                $contact_id,
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
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
