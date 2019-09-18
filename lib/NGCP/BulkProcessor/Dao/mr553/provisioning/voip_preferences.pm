package NGCP::BulkProcessor::Dao::mr553::provisioning::voip_preferences;
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

);
    #$ALLOWED_CLIS_ATTRIBUTE
    #$CLI_ATTRIBUTE
    #$AC_ATTRIBUTE
    #$CC_ATTRIBUTE
    #$ACCOUNT_ID_ATTRIBUTE
    #
    #$NCOS_ID_ATTRIBUTE
    #$ADM_NCOS_ID_ATTRIBUTE
    #
    #$GPPx_ATTRIBUTE
    #
    #$PEER_AUTH_USER
    #$PEER_AUTH_PASS
    #$PEER_AUTH_REALM
    #$PEER_AUTH_REGISTER
    #$FORCE_INBOUND_CALLS_TO_PEER
    #
    #$ALLOWED_IPS_GRP_ATTRIBUTE
    #$CONCURRENT_MAX_TOTAL_ATTRIBUTE
    #$CONCURRENT_MAX_PER_ACCOUNT
    #
    #@CF_ATTRIBUTES
    #$RINGTIMEOUT_ATTRIBUTE
    #
    #$EXTENDED_DIALING_MODE_ATTRIBUTE
    #$E164_TO_RURI_ATTRIBUTE
    #$SERIAL_FORKING_BY_Q_VALUE_ATTRIBUTE
    #
    #$CLOUD_PBX_ATTRIBUTE
    #$CLOUD_PBX_BASE_CLI_ATTRIBUTE
    #$CLOUD_PBX_HUNT_POLICY_ATTRIBUTE
    #$MUSIC_ON_HOLD_ATTRIBUTE
    #$SHARED_BUDDYLIST_VISIBILITY_ATTRIBUTE

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

our $ALLOWED_IPS_GRP_ATTRIBUTE = 'allowed_ips_grp';

#our $ALLOWED_CLIS_ATTRIBUTE = 'allowed_clis';
#our $CLI_ATTRIBUTE = 'cli';
#our $AC_ATTRIBUTE = 'ac';
#our $CC_ATTRIBUTE = 'cc';
#our $ACCOUNT_ID_ATTRIBUTE = 'account_id';
#
#our $NCOS_ID_ATTRIBUTE = 'ncos_id';
#our $ADM_NCOS_ID_ATTRIBUTE = 'adm_ncos_id';
#our $GPPx_ATTRIBUTE = 'gpp';
#
#our $PEER_AUTH_USER = 'peer_auth_user';
#our $PEER_AUTH_PASS = 'peer_auth_pass';
#our $PEER_AUTH_REALM = 'peer_auth_realm';
#our $PEER_AUTH_REGISTER = 'peer_auth_register';
#our $FORCE_INBOUND_CALLS_TO_PEER = 'force_inbound_calls_to_peer';
#
#our $CONCURRENT_MAX_TOTAL_ATTRIBUTE = 'concurrent_max_total';
#our $CONCURRENT_MAX_PER_ACCOUNT_ATTRIBUTE = 'concurrent_max_per_account';
#our $CLIR_ATTRIBUTE = 'clir';
#
#our @CF_ATTRIBUTES = qw(cfu cft cfna cfb); #skip sms for now
#
#our $RINGTIMEOUT_ATTRIBUTE = 'ringtimeout';
#
#our $EXTENDED_DIALING_MODE_ATTRIBUTE = 'extended_dialing_mode';
#our $E164_TO_RURI_ATTRIBUTE = 'e164_to_ruri';
#our $SERIAL_FORKING_BY_Q_VALUE_ATTRIBUTE = 'serial_forking_by_q_value';
#
#our $CLOUD_PBX_ATTRIBUTE = 'cloud_pbx';
#our $CLOUD_PBX_BASE_CLI_ATTRIBUTE = 'cloud_pbx_base_cli';
#our $CLOUD_PBX_HUNT_POLICY_ATTRIBUTE = 'cloud_pbx_hunt_policy';
#our $MUSIC_ON_HOLD_ATTRIBUTE = 'music_on_hold';
#our $SHARED_BUDDYLIST_VISIBILITY_ATTRIBUTE = 'shared_buddylist_visibility';

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
