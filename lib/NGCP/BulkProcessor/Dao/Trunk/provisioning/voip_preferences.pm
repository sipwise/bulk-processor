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

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_rewrite_rule_sets qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences_enum qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    findby_attribute
    findall
    findby_id

    $ALLOWED_CLIS_ATTRIBUTE
    $CLI_ATTRIBUTE
    $AC_ATTRIBUTE
    $CC_ATTRIBUTE
    $ACCOUNT_ID_ATTRIBUTE

    $NCOS_ID_ATTRIBUTE
    $ADM_NCOS_ID_ATTRIBUTE

    $GPPx_ATTRIBUTE
    %DPID_ATTRIBUTES

    $PEER_AUTH_USER
    $PEER_AUTH_PASS
    $PEER_AUTH_REALM
    $PEER_AUTH_REGISTER
    $FORCE_INBOUND_CALLS_TO_PEER

    $ALLOWED_IPS_GRP_ATTRIBUTE
    $MAN_ALLOWED_IPS_GRP_ATTRIBUTE
    $CONCURRENT_MAX_TOTAL_ATTRIBUTE
    $CONCURRENT_MAX_PER_ACCOUNT

    @CF_ATTRIBUTES
    $RINGTIMEOUT_ATTRIBUTE

    $EXTENDED_DIALING_MODE_ATTRIBUTE
    $E164_TO_RURI_ATTRIBUTE
    $SERIAL_FORKING_BY_Q_VALUE_ATTRIBUTE

    $CLOUD_PBX_ATTRIBUTE
    $CLOUD_PBX_BASE_CLI_ATTRIBUTE
    $CLOUD_PBX_HUNT_POLICY_ATTRIBUTE
    $MUSIC_ON_HOLD_ATTRIBUTE
    $SHARED_BUDDYLIST_VISIBILITY_ATTRIBUTE

    $CDR_EXPORT_SCLIDUI_RWRS_ID_ATTRIBUTE
    $EMERGENCY_MAPPING_CONTAINER_ID_ATTRIBUTE

    $SOUND_SET_ATTRIBUTE
    $CONTRACT_SOUND_SET_ATTRIBUTE
    $HEADER_RULE_SET_ATTRIBUTE
    
    $EMERGENCY_PREFIX_ATTRIBUTE
    
    $BLOCK_IN_CLIR_ATTRIBUTE
    $BLOCK_OUT_OVERRIDE_PIN_ATTRIBUTE
    $ADM_BLOCK_OUT_OVERRIDE_PIN_ATTRIBUTE

    $BOOLEAN_DATA_TYPE
);
#$FORCE_OUTBOUND_CALLS_TO_PEER
#$ADM_CF_NCOS_ID_ATTRIBUTE

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

our $ALLOWED_CLIS_ATTRIBUTE = 'allowed_clis';
our $CLI_ATTRIBUTE = 'cli';
our $AC_ATTRIBUTE = 'ac';
our $CC_ATTRIBUTE = 'cc';
our $ACCOUNT_ID_ATTRIBUTE = 'account_id';

our $NCOS_ID_ATTRIBUTE = 'ncos_id';
our $ADM_NCOS_ID_ATTRIBUTE = 'adm_ncos_id';
#our $ADM_CF_NCOS_ID_ATTRIBUTE = 'adm_cf_ncos_id';
our $GPPx_ATTRIBUTE = 'gpp';

our %DPID_ATTRIBUTES = map { 'rewrite_' . $_ => $_; } @NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_rewrite_rule_sets::DPID_FIELDS;

our $PEER_AUTH_USER = 'peer_auth_user';
our $PEER_AUTH_PASS = 'peer_auth_pass';
our $PEER_AUTH_REALM = 'peer_auth_realm';
our $PEER_AUTH_REGISTER = 'peer_auth_register';
our $FORCE_INBOUND_CALLS_TO_PEER = 'force_inbound_calls_to_peer';
#our $FORCE_OUTBOUND_CALLS_TO_PEER = 'force_outbound_calls_to_peer';

our $ALLOWED_IPS_GRP_ATTRIBUTE = 'allowed_ips_grp';
our $MAN_ALLOWED_IPS_GRP_ATTRIBUTE = 'man_allowed_ips_grp';

our $CONCURRENT_MAX_TOTAL_ATTRIBUTE = 'concurrent_max_total';
our $CONCURRENT_MAX_PER_ACCOUNT_ATTRIBUTE = 'concurrent_max_per_account';
our $CLIR_ATTRIBUTE = 'clir';

our @CF_ATTRIBUTES = qw(cfu cft cfna cfb cfo cfr cfs);

our $RINGTIMEOUT_ATTRIBUTE = 'ringtimeout';

our $EXTENDED_DIALING_MODE_ATTRIBUTE = 'extended_dialing_mode';
our $E164_TO_RURI_ATTRIBUTE = 'e164_to_ruri';
our $SERIAL_FORKING_BY_Q_VALUE_ATTRIBUTE = 'serial_forking_by_q_value';

our $CLOUD_PBX_ATTRIBUTE = 'cloud_pbx';
our $CLOUD_PBX_BASE_CLI_ATTRIBUTE = 'cloud_pbx_base_cli';
our $CLOUD_PBX_HUNT_POLICY_ATTRIBUTE = 'cloud_pbx_hunt_policy';
our $MUSIC_ON_HOLD_ATTRIBUTE = 'music_on_hold';
our $SHARED_BUDDYLIST_VISIBILITY_ATTRIBUTE = 'shared_buddylist_visibility';

our $CDR_EXPORT_SCLIDUI_RWRS_ID_ATTRIBUTE = 'cdr_export_sclidui_rwrs_id';
our $EMERGENCY_MAPPING_CONTAINER_ID_ATTRIBUTE = 'emergency_mapping_container_id';
our $SOUND_SET_ATTRIBUTE = 'sound_set';
our $CONTRACT_SOUND_SET_ATTRIBUTE = 'contract_sound_set';
our $HEADER_RULE_SET_ATTRIBUTE = 'header_rule_set';

our $EMERGENCY_PREFIX_ATTRIBUTE = 'emergency_prefix';

our $BLOCK_IN_CLIR_ATTRIBUTE = 'block_in_clir';

our $BLOCK_OUT_OVERRIDE_PIN_ATTRIBUTE = 'block_out_override_pin';
our $ADM_BLOCK_OUT_OVERRIDE_PIN_ATTRIBUTE = 'adm_block_out_override_pin';

our $BOOLEAN_DATA_TYPE = 'boolean';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_attribute {

    my ($attribute,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('attribute') . ' = ?';
    my @params = ($attribute);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findby_id {

    my ($attribute_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($attribute_id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findall {

    my ($load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my $rows = $db->db_get_all_arrayref($stmt);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...
            $record->{enums} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences_enum::findby_attributeid($record->{id});

            push @records,$record;
        }
    }

    return \@records;

}

sub has_enum_default {
    my $self = shift;
    my $type = shift;
    foreach my $enum (@{$self->{enums}}) {
        if ($self->{$type} == 1
            and $enum->{$type} == 1
            and $enum->{default_val} == 1
            and defined $enum->{value}) {
            return 1;
        }
    }
    return 0;
}

sub is_boolean {
    my $self = shift;
    if ($self->{data_type} eq $BOOLEAN_DATA_TYPE) {
        return 1;
    }
    return 0;
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
