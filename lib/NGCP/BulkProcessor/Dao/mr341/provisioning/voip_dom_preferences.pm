package NGCP::BulkProcessor::Dao::mr341::provisioning::voip_dom_preferences;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::mr341::provisioning::voip_allowed_ip_groups qw();
use NGCP::BulkProcessor::Dao::mr341::provisioning::voip_preferences qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findby_domain
    source_findby_attributesused

    $TRUE
    $FALSE
);

my $tablename = 'voip_dom_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'domain_id',
    'attribute_id',
    'value',
    'modify_timestamp',
];

my $indexes = {};

my $insert_unique_fields = [];

our $TRUE = 1;
our $FALSE = undef;

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

sub source_findby_domain {

    my ($source_dbs,$domain) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT v.*,a.attribute FROM ' . $table . ' v JOIN ' .
            $db->tableidentifier('voip_preferences') . ' a ON v.attribute_id = a.id' .
            ' JOIN provisioning.voip_domains d ON v.domain_id = d.id' .
            ' WHERE d.domain = ?';
    my @params = ($domain);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_findby_attributesused {

    my ($source_dbs) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT DISTINCT(a.attribute) FROM ' . $table . ' v JOIN ' .
            $db->tableidentifier('voip_preferences') . ' a ON v.attribute_id = a.id';
    my @params = ();

    return $db->db_get_col($stmt,@params);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records = (); # : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...
            $record->{attribute} = $row->{attribute};

            if ($record->{attribute} eq $NGCP::BulkProcessor::Dao::mr341::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE) {
                my @allowed_ip_groups = map { $_->{ipnet}; } @{NGCP::BulkProcessor::Dao::mr341::provisioning::voip_allowed_ip_groups::source_findby_group_id(
                    $source_dbs,$record->{value})};
                $record->{allowed_ip_groups} = \@allowed_ip_groups;
            }

            push @records,$record;
        }
    }

    return \@records;

}

1;
