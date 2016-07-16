package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences;
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
);

my $tablename = 'voip_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'voip_preference_groups_id',
    'attribute',
    'label',
    'type',
    'max_occur',
    'usr_pref',
    'prof_pref',
    'dom_pref',
    'peer_pref',
    'contract_pref',
    'contract_location_pref',
    'modify_timestamp',
    'internal',
    'expose_to_customer',
    'data_type',
    'read_only',
    'description',
];

my $indexes = {};

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
