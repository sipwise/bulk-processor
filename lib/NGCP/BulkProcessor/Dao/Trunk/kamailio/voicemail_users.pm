package NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users;
use strict;

## no critic

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

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

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

    return checktableinfo($get_db,
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
