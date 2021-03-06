package NGCP::BulkProcessor::Dao::mr102::billing::resellers;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db

);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

#use NGCP::BulkProcessor::Dao::mr102::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr102::billing::domain_resellers qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findall

    $TERMINATED_STATE
);

my $tablename = 'resellers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'name',
    'status',
];

my $indexes = {};

our $TERMINATED_STATE = 'terminated';

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

    my $source_db = $source_dbs->{billing_db};
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
            $record = __PACKAGE__->source_new($source_dbs->{billing_db},$row);

            # transformations go here ...
            #$record->{contract} = NGCP::BulkProcessor::Dao::mr102::billing::contracts::source_findby_id($source_dbs,$record->{contract_id});

            my @domains = ();
            foreach my $domain_reseller (@{NGCP::BulkProcessor::Dao::mr102::billing::domain_resellers::source_findby_resellerid($source_dbs,$record->{id})}) {
                push(@domains,$domain_reseller->{domain});
            }
            $record->{domains} = \@domains;

            #$record->{ncos_levels} = NGCP::BulkProcessor::Dao::mr102::billing::ncos_levels::source_findby_resellerid($source_dbs,$record->{id});

            push @records,$record;
        }
    }

    return \@records;

}

1;
