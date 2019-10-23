package NGCP::BulkProcessor::Dao::mr103::provisioning::voip_subscribers;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::mr103::openser::voicemail_users qw();
use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_fax_preferences qw();
use NGCP::BulkProcessor::Dao::mr103::provisioning::voip_fax_destinations qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    source_findby_uuid
);

my $tablename = 'voip_subscribers';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'username',
    'domain_id',

    'uuid',
    'password',
    'timezone',

    'admin',
    'account_id',
    'webusername',
    'webpassword',

    'autoconf_displayname',
    'autoconf_group_id',

    'modify_timestamp',
    'create_timestamp',
];

my $indexes = {};

my $insert_unique_fields = [];

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

sub source_findby_uuid {

    my ($source_dbs,$uuid) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('uuid') . ' = ?';
    my @params = ($uuid);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs)->[0];

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...
            $record->{voip_dbaliases} = NGCP::BulkProcessor::Dao::mr103::provisioning::voip_dbaliases::source_findby_subscriberid($source_dbs,$record->{id});
            $record->{voip_usr_preferences} = NGCP::BulkProcessor::Dao::mr103::provisioning::voip_usr_preferences::source_findby_subscriberid($source_dbs,$record->{id});

            $record->{voicemail_users} = NGCP::BulkProcessor::Dao::mr103::openser::voicemail_users::source_findby_customerid($source_dbs,$record->{uuid});

            $record->{voip_fax_preferences} = NGCP::BulkProcessor::Dao::mr103::provisioning::voip_fax_preferences::source_findby_subscriberid($source_dbs,$record->{id});
            $record->{voip_fax_destinations} = NGCP::BulkProcessor::Dao::mr103::provisioning::voip_fax_destinations::source_findby_subscriberid($source_dbs,$record->{id});

            #delete $record->{account_id};
            #delete $record->{autoconf_displayname};
            #delete $record->{autoconf_group_id};
            #delete $record->{domain_id};
            #delete $record->{id};

            push @records,$record;
        }
    }

    return \@records;

}

1;
