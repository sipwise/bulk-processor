package NGCP::BulkProcessor::Dao::mr103::provisioning::voip_preferences;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
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

    $ALLOWED_IPS_GRP_ATTRIBUTE
    $MAN_ALLOWED_IPS_GRP_ATTRIBUTE

);

my $tablename = 'voip_preferences';
my $get_db = \&get_provisioning_db;

id | attribute      | type | max_occur | modify_timestamp    | internal

my $expected_fieldnames = [
    'id',
    'attribute',
    'type',
    'max_occur',
    'modify_timestamp',
    'internal',
];

my $indexes = {};

our $ALLOWED_IPS_GRP_ATTRIBUTE = 'allowed_ips_grp';
our $MAN_ALLOWED_IPS_GRP_ATTRIBUTE = 'man_allowed_ips_grp';

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

1;
