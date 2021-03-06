package NGCP::BulkProcessor::Dao::Trunk::billing::email_templates;
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
    copy_row
    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    insert_row

);

my $tablename = 'email_templates';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
  'id',
  'reseller_id',
  'name',
  'from_email',
  'subject',
  'body',
  'attachment_name',
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
        my ($reseller_id,
            $name,
            $from_email,
            $subject,
            $body,
            $attachment_name) = @params{qw/
            reseller_id
            name
            from_email
            subject
            body
            attachment_name
        /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('reseller_id') . ', ' .
                $db->columnidentifier('name') . ', ' .
                $db->columnidentifier('from_email') . ', ' .
                $db->columnidentifier('subject') . ', ' .
                $db->columnidentifier('body') . ', ' .
                $db->columnidentifier('attachment_name') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?)',
                $reseller_id,
                $name,
                $from_email,
                $subject,
                $body,
                $attachment_name,
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
