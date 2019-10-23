package NGCP::BulkProcessor::Dao::mr103::provisioning::voip_usr_preferences;
use strict;

## no critic

use threads::shared;

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

use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_allowed_ip_groups qw();
use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::mr103::billing::ncos_levels qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findby_subscriberid

    $TRUE
    $FALSE
);
#source_findby_attributesused

my $tablename = 'voip_usr_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'subscriber_id',
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
    my $self = NGCP::BulkProcessor::SqlRecord->new_shared($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findby_subscriberid {

    my ($source_dbs,$subscriber_id) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT v.*,a.attribute FROM ' . $table . ' v JOIN ' .
            $db->tableidentifier('voip_preferences') . ' a ON v.attribute_id = a.id WHERE ' .
            'v.subscriber_id = ?';
    my @params = ($subscriber_id);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...
            $record->{attribute} = $row->{attribute};

            if ($record->{attribute} eq $NGCP::BulkProcessor::Dao::mr103::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE) {
                my @allowed_ip_groups : shared = map { $_->{ipnet}; } @{NGCP::BulkProcessor::Dao::mr103::provisioning::voip_allowed_ip_groups::source_findby_group_id(
                    $source_dbs,$record->{value})};
                $record->{allowed_ip_groups} = \@allowed_ip_groups;
                delete $record->{value};
            }
            if ($record->{attribute} eq $NGCP::BulkProcessor::Dao::mr103::provisioning::voip_preferences::MAN_ALLOWED_IPS_GRP_ATTRIBUTE) {
                my @allowed_ip_groups : shared = map { $_->{ipnet}; } @{NGCP::BulkProcessor::Dao::mr103::provisioning::voip_allowed_ip_groups::source_findby_group_id(
                    $source_dbs,$record->{value})};
                $record->{man_allowed_ip_groups} = \@allowed_ip_groups;
                delete $record->{value};
            }
            if ($record->{attribute} eq $NGCP::BulkProcessor::Dao::mr103::provisioning::voip_preferences::NCOS_ID_ATTRIBUTE) {
                 my %ncos : shared = ( %{NGCP::BulkProcessor::Dao::mr103::billing::ncos_levels::source_getuniquename($source_dbs,$record->{value})} );
                 $record->{ncos} = \%ncos;
                 delete $record->{value};
            }
            
            delete $record->{attribute_id};
            delete $record->{subscriber_id};
            delete $record->{id};

            push @records,$record;
        }
    }

    return \@records;

}

1;
