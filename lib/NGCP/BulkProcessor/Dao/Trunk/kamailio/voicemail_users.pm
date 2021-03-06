package NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
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

    findby_customerid
    insert_row
);

my $tablename = 'voicemail_users';
my $get_db = \&get_kamailio_db;

my $expected_fieldnames = [
    'uniqueid',
    'customer_id',
    'context',
    'mailbox',
    'password',
    'fullname',
    'email',
    'pager',
    'tz',
    'attach',
    'saycid',
    'dialout',
    'callback',
    'review',
    'operator',
    'envelope',
    'sayduration',
    'saydurationm',
    'sendvoicemail',
    'delete',
    'nextaftercmd',
    'forcename',
    'forcegreetings',
    'hidefromdir',
    'stamp',
];

my $indexes = {};

my $insert_unique_fields = [];

my $default_tz = 'vienna';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_customerid {

    my ($uuid,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('customer_id') . ' = ?';
    my @params = ($uuid);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

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
        my ($customer_id,
            $mailbox,
            $password) = @params{qw/
                customer_id
                mailbox
                password
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('customer_id') . ', ' .
                $db->columnidentifier('email') . ', ' .
                $db->columnidentifier('mailbox') . ', ' .
                $db->columnidentifier('password') . ', ' .
                $db->columnidentifier('tz') . ') VALUES (' .
                '?, ' .
                '\'\', ' .
                '?, ' .
                '?, ' .
                '\'' . $default_tz . '\')',
                $customer_id,
                $mailbox,
                $password,
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
            $record->load_relation($load_recursive,'voicemail_spool','NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_spool::findby_mailboxuser',$record->{mailbox},$load_recursive);

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
