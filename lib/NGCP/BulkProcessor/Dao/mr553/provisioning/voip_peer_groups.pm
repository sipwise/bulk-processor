package NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_groups;
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

use NGCP::BulkProcessor::Dao::mr553::billing::contracts qw();

use NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_hosts qw();
use NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_rules qw();
use NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_inbound_rules qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findall


);

my $tablename = 'voip_peer_groups';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'name',
    'priority',
    'description',
    'peering_contract_id',
    'has_inbound_rules',
    #'time_set_id',
];

my $indexes = {};



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

sub source_new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findall {

    my ($source_dbs) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my $rows = $db->db_get_all_arrayref($stmt);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records = (); # : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...

            $record->{contract} = NGCP::BulkProcessor::Dao::mr553::billing::contracts::source_findby_id($source_dbs,$record->{peering_contract_id});

            $record->{hosts} = NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_hosts::source_findby_groupid($source_dbs,$record->{id});
            $record->{rules} = NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_rules::source_findby_groupid($source_dbs,$record->{id});
            $record->{inbound_rules} = NGCP::BulkProcessor::Dao::mr553::provisioning::voip_peer_inbound_rules::source_findby_groupid($source_dbs,$record->{id});

            #$record->{contract} = NGCP::BulkProcessor::Dao::mr553::billing::contracts::source_findby_id($source_dbs,$record->{contract_id});

            #my @domains = ();
            #foreach my $domain_reseller (@{NGCP::BulkProcessor::Dao::mr553::billing::domain_resellers::source_findby_resellerid($source_dbs,$record->{id})}) {
            #    push(@domains,$domain_reseller->{domain});
            #}
            #$record->{domains} = \@domains;

            #$record->{email_templates} = NGCP::BulkProcessor::Dao::mr553::billing::email_templates::source_findby_resellerid($source_dbs,$record->{id});

            #domains
            #email_templates

            push @records,$record;
        }
    }

    return \@records;

}

1;
