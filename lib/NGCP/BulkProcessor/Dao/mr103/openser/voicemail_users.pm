package NGCP::BulkProcessor::Dao::mr341::kamailio::voicemail_users;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger

);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
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

    source_findby_customerid
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

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

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

sub source_findby_customerid {

    my ($source_dbs,$uuid) = @_;

    my $source_db = $source_dbs->{kamailio_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('customer_id') . ' = ?';
    my @params = ($uuid);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{kamailio_db},$row);

            # transformations go here ...

            push @records,$record;
        }
    }

    return \@records;

}

1;
