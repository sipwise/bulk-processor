package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
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

    update_row
    insert_row

    delete_callids
    countby_ratingstatus

);
#process_records
#delete_ids

my $tablename = 'cdr';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
"id",
"update_time",
"source_user_id",
"source_provider_id",
"source_external_subscriber_id",
"source_external_contract_id",
"source_account_id",
"source_user",
"source_domain",
"source_cli",
"source_clir",
"source_ip",
"source_gpp0",
"source_gpp1",
"source_gpp2",
"source_gpp3",
"source_gpp4",
"source_gpp5",
"source_gpp6",
"source_gpp7",
"source_gpp8",
"source_gpp9",
"source_lnp_prefix",
"destination_user_id",
"destination_provider_id",
"destination_external_subscriber_id",
"destination_external_contract_id",
"destination_account_id",
"destination_user",
"destination_domain",
"destination_user_dialed",
"destination_user_in",
"destination_domain_in",
"destination_gpp0",
"destination_gpp1",
"destination_gpp2",
"destination_gpp3",
"destination_gpp4",
"destination_gpp5",
"destination_gpp6",
"destination_gpp7",
"destination_gpp8",
"destination_gpp9",
"destination_lnp_prefix",
"peer_auth_user",
"peer_auth_realm",
"call_type",
"call_status",
"call_code",
"init_time",
"start_time",
"duration",
"call_id",
"source_carrier_cost",
"source_reseller_cost",
"source_customer_cost",
"source_carrier_free_time",
"source_reseller_free_time",
"source_customer_free_time",
"source_carrier_billing_fee_id",
"source_reseller_billing_fee_id",
"source_customer_billing_fee_id",
"source_carrier_billing_zone_id",
"source_reseller_billing_zone_id",
"source_customer_billing_zone_id",
"destination_carrier_cost",
"destination_reseller_cost",
"destination_customer_cost",
"destination_carrier_free_time",
"destination_reseller_free_time",
"destination_customer_free_time",
"destination_carrier_billing_fee_id",
"destination_reseller_billing_fee_id",
"destination_customer_billing_fee_id",
"destination_carrier_billing_zone_id",
"destination_reseller_billing_zone_id",
"destination_customer_billing_zone_id",
"frag_carrier_onpeak",
"frag_reseller_onpeak",
"frag_customer_onpeak",
"is_fragmented",
"split",
"rated_at",
"rating_status",
"exported_at",
"export_status",
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


sub delete_callids {

    my ($xa_db,$callids) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('call_id') . ' IN (' . substr(',?' x scalar @$callids,1) . ')';
    my @params = @$callids;

    my $count;
    if ($count = $xa_db->db_do($stmt,@params)) {
        rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
        return 0;
    }

}

sub countby_ratingstatus {

    my ($rating_status) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if (defined $rating_status) {
        push(@terms,$db->columnidentifier('rating_status') . ' = ?');
        push(@params,$rating_status);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

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
        #my %params = @_;
        #my ($contract_id,
        #    $domain_id,
        #    $username,
        #    $uuid) = @params{qw/
        #        contract_id
        #        domain_id
        #        username
        #        uuid
        #    /};
        #
        #if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
        #        $db->columnidentifier('contact_id') . ', ' .
        #        $db->columnidentifier('contract_id') . ', ' .
        #        $db->columnidentifier('domain_id') . ', ' .
        #        $db->columnidentifier('external_id') . ', ' .
        #        $db->columnidentifier('primary_number_id') . ', ' .
        #        $db->columnidentifier('status') . ', ' .
        #        $db->columnidentifier('username') . ', ' .
        #        $db->columnidentifier('uuid') . ') VALUES (' .
        #        'NULL, ' .
        #        '?, ' .
        #        '?, ' .
        #        'NULL, ' .
        #        'NULL, ' .
        #        '\'' . $ACTIVE_STATE . '\', ' .
        #        '?, ' .
        #        '?)',
        #        $contract_id,
        #        $domain_id,
        #        $username,
        #        $uuid,
        #    )) {
        #    rowinserted($db,$tablename,getlogger(__PACKAGE__));
        #    return $xa_db->db_last_insert_id();
        #}
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
