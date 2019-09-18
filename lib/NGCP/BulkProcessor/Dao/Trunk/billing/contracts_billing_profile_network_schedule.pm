package NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network_schedule;
use strict;

## no critic

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
use NGCP::BulkProcessor::Calendar qw(datetime_to_string);

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    append_billing_mappings
);

my $tablename = 'contracts_billing_profile_network_schedule';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'profile_network_id',
    'effective_start_time',
];

my $indexes = {};

#my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub append_billing_mappings {

    my ($xa_db,$contract_id,$mappings_to_create,$now,$delete_mappings) = @_;
    return unless $mappings_to_create;

    check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::contracts_billing_profile_network::check_table();
    my $db = &$get_db();
    $xa_db //= $db;

    my $mappings = '';
    foreach my $mapping (@$mappings_to_create) {
        $mappings .= (defined $mapping->{start_date} ? _datetime_to_string($mapping->{start_date}) : '') . ',';
        $mappings .= (defined $mapping->{end_date} ? _datetime_to_string($mapping->{end_date}) : '') . ',';
        $mappings .= (defined $mapping->{billing_profile_id} ? $mapping->{billing_profile_id} : '') . ',';
        $mappings .= (defined $mapping->{network_id} ? $mapping->{network_id} : '') . ',';
        $mappings .= ';'; #last = 1 by default
    }

    $xa_db->db_do('call billing.schedule_contract_billing_profile_network(?,?,?)',
        $contract_id,
        ((defined $now and $delete_mappings) ? _datetime_to_string($now) : undef),
        $mappings
    );

}

sub _datetime_to_string {
    my $dt = shift;
    if (ref $dt) {
        return datetime_to_string($dt);
    }
    return $dt;
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
