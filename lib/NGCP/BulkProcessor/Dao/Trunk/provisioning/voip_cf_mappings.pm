package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings;
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

    countby_subscriberid_type
    $CFB_TYPE
    $CFT_TYPE
    $CFU_TYPE
    $CFNA_TYPE
);

my $tablename = 'voip_cf_mappings';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'subscriber_id',
    'type',
    'destination_set_id',
    'time_set_id',
];

my $indexes = {};

our $CFB_TYPE = 'cfb';
our $CFT_TYPE = 'cft';
our $CFU_TYPE = 'cfu';
our $CFNA_TYPE = 'cfna';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub countby_subscriberid_type {

    my ($subscriber_id,$type,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($subscriber_id) {
        push(@terms,$db->columnidentifier('subscriber_id') . ' = ?');
        push(@params,$subscriber_id);
    }
    if ($type) {
        push(@terms,$db->columnidentifier('type') . ' = ?');
        push(@params,$type);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
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
