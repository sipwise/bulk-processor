package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row

    findby_uuid
);

my $tablename = 'voip_subscribers';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'username',
    'domain_id',
    'uuid',
    'password',
    'admin',
    'account_id',
    'webusername',
    'webpassword',
    'is_pbx_pilot',
    'is_pbx_group',
    'pbx_hunt_policy',
    'pbx_hunt_timeout',
    'pbx_extension',
    'profile_set_id',
    'profile_id',
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

sub findby_uuid {

    my ($xa_db,$uuid,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('uuid') . ' = ?';
    my @params = ($uuid);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($account_id,
            $domain_id,
            $password,
            $username,
            $uuid,
            $webpassword,
            $webusername) = @params{qw/
                account_id
                domain_id
                password
                username
                uuid
                webpassword
                webusername
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('account_id') . ', ' .
                $db->columnidentifier('admin') . ', ' .
                $db->columnidentifier('create_timestamp') . ', ' .
                $db->columnidentifier('domain_id') . ', ' .
                $db->columnidentifier('is_pbx_group') . ', ' .
                $db->columnidentifier('is_pbx_pilot') . ', ' .
                $db->columnidentifier('password') . ', ' .
                $db->columnidentifier('pbx_extension') . ', ' .
                $db->columnidentifier('pbx_hunt_policy') . ', ' .
                $db->columnidentifier('pbx_hunt_timeout') . ', ' .
                $db->columnidentifier('profile_id') . ', ' .
                $db->columnidentifier('profile_set_id') . ', ' .
                $db->columnidentifier('username') . ', ' .
                $db->columnidentifier('uuid') . ', ' .
                $db->columnidentifier('webpassword') . ', ' .
                $db->columnidentifier('webusername') . ') VALUES (' .
                '?, ' .
                '\'0\', ' .
                'NOW(), ' .
                '?, ' .
                '\'0\', ' .
                '\'0\', ' .
                '?, ' .
                'NULL, ' .
                'NULL, ' .
                'NULL, ' .
                'NULL, ' .
                'NULL, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?)',
                $account_id,
                $domain_id,
                $password,
                $username,
                $uuid,
                $webpassword,
                $webusername
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

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
