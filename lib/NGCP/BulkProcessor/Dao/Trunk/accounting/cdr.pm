package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr;
use strict;

#use Tie::IxHash;
## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    copy_row

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_mos_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_tag_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_relation_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_cash_balance_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_time_balance_data qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    settablename
    check_table

    update_row
    insert_row

    delete_callids
    countby_ratingstatus

    findby_callidprefix
    process_unexported
    process_fromto

    get_callidprefix
    findby_callid

    $OK_CALL_STATUS

    $CFU_CALL_TYPE
    $CFB_CALL_TYPE

    findby_id
    get_cdrid_range
    
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
"source_user_out",
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
"source_lnp_type",
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
"destination_user_out",
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
"destination_lnp_type",
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

my @callid_suffixes = ();
my $PBXSUFFIX = '_pbx-1';
push(@callid_suffixes,$PBXSUFFIX);
my $XFERSUFFIX = '_xfer-1';
push(@callid_suffixes,$XFERSUFFIX);

our $OK_CALL_STATUS = 'ok';

our $CFU_CALL_TYPE = 'cfu';
our $CFB_CALL_TYPE = 'cfb';

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_id {

    my ($id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub get_cdrid_range {
    
    my ($id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT min(id) as min, max(id) as max FROM ' . $table;
    my @params = ();
    my $row = $db->db_get_row($stmt,@params);

    return $row;
    
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

sub get_callidprefix {

    my ($call_id) = @_;
    my $suffixre = '(' . join('|', map { quotemeta($_); } @callid_suffixes) . ')+$';
    $call_id =~ s/$suffixre//g;
    return $call_id

}

sub findby_callidprefix {

    my ($xa_db,$call_id,$alias,$joins,$conditions,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    $call_id = get_callidprefix($call_id);
    $call_id =~ s/%/\\%/g;

    my @conditions = @{$conditions // []};
    push(@conditions,{ $table . '.call_id' => { 'LIKE' => '?' } });
    my $stmt = 'SELECT ' . join(',', map { $table . '.' . $db->columnidentifier($_); } @$expected_fieldnames) . ' ' .
        _get_export_stmt($db,$alias,$joins,\@conditions) .
        ' ORDER BY LENGTH(' . $table . '.call_id' . ') ASC, ' . $table . '.start_time ASC';
    my @params = ($call_id . '%');
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_callid {

    my ($xa_db,$call_id,$alias,$joins,$conditions,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my @conditions = @{$conditions // []};
    push(@conditions,{ $table . '.call_id' => { '=' => '?' } });
    my $stmt = 'SELECT ' . join(',', map { $table . '.' . $db->columnidentifier($_); } @$expected_fieldnames) . ' ' .
        _get_export_stmt($db,$alias,$joins,\@conditions) .
        ' ORDER BY ' . $table . '.start_time ASC';
    my @params = ($call_id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;

    my ($data,$insert_ignore) = @_;
    check_table();
    if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
        return $xa_db->db_last_insert_id();
    }
    return undef;

}

sub process_unexported {

    my %params = @_;
    my ($process_code,
        $name,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $blocksize,
        $alias,
        $joins,
        $conditions,
        $loop,
        #$sort,
        $limit) = @params{qw/
            process_code
            name
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            blocksize
            alias
            joins
            conditions
            loop
            limit
        /};
    #sort

    check_table();
    NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $select_stmt;
    my $count_stmt;
    my $select_format = 'SELECT ' . ($alias ? $alias : $table) . '.id, ' . ($alias ? $alias : $table) . '.call_id, ' . ($alias ? $alias : $table) . '.start_time %s'; # ORDER BY ' . $table . '.' . $db->columnidentifier('id');
    my $count_format = 'SELECT COUNT(1) %s';
    if (defined $limit) {
        $select_format .= ' LIMIT ' . $limit;
        $count_format = sprintf($count_format,'FROM (SELECT 1 %s LIMIT ' . $limit . ') as q');
    }
    
    if ($static_context) {
        $static_context->{part} = 'A';
        $select_stmt = '(' . sprintf($select_format,_get_export_stmt_part($db,$static_context,$alias,$joins,$conditions,1));
        $count_stmt = '(' . sprintf($count_format,_get_export_stmt_part($db,$static_context,$alias,$joins,$conditions,0));
        $select_stmt .= ') UNION ALL (';
        $count_stmt .= ') + (';
        $static_context->{part} = 'B';
        my $b = _get_export_stmt_part($db,$static_context,$alias,$joins,$conditions,1);
        $select_stmt .= sprintf($select_format,_get_export_stmt_part($db,$static_context,$alias,$joins,$conditions,1)) . ')';
        $select_stmt = 'SELECT id, call_id, start_time FROM (' . $select_stmt . ') as q ORDER BY id';
        $count_stmt .= sprintf($count_format,_get_export_stmt_part($db,$static_context,$alias,$joins,$conditions,0)) . ')';
        if (defined $limit) {
            $count_stmt = 'SELECT LEAST(' . $count_stmt . ', ' . $limit . ')';
            $select_stmt .= ' LIMIT ' . $limit;
        } else {
            $count_stmt = 'SELECT ' . $count_stmt;
        }
        delete $static_context->{part};
    } else {
        $select_stmt = sprintf($select_format,_get_export_stmt_part($db,undef,$alias,$joins,$conditions,1));
        $count_stmt = sprintf($count_format,_get_export_stmt_part($db,undef,$alias,$joins,$conditions,0));
    }
    
    return process_table(
        name                        => $name,
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                #my %cdr_id_map = ();
                #tie(%cdr_id_map, 'Tie::IxHash');
                #if ($rowblock) {
                #    foreach my $record (@$rowblock) {
                #        $cdr_id_map{$record->[0]} = $record->[1];
                #    }
                #}
                #return 0 if $row_offset >= $limit;
                return &$process_code($context,$rowblock,$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        blocksize                   => $blocksize,
        select                      => $select_stmt,
        selectcount                 => $count_stmt,
        loop                        => $loop,
    );
}

sub process_fromto {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $blocksize,
        $from,
        $to) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            blocksize
            from
            to
        /};
    #sort

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = '';
    if ($from or $to) {
        $stmt .= ' WHERE ';
        my @terms = ();
        if ($from) {
            push(@terms,$db->columnidentifier('start_time') . ' >= UNIX_TIMESTAMP("' . $from . '")');
        }
        if ($to) {
            push(@terms,$db->columnidentifier('start_time') . ' < UNIX_TIMESTAMP("' . $to . '")');
        }
        $stmt .= join(' AND ',@terms);
    }


    return process_table(
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,$rowblock,$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        blocksize                   => $blocksize,
        select                      => 'SELECT * FROM ' . $table . $stmt . ' ORDER BY ' . $db->columnidentifier('id'),
        selectcount                 => 'SELECT COUNT(1) FROM ' . $table . $stmt,
    );
}

sub _get_export_stmt {

    my ($db,$alias,$joins,$conditions) = @_;
    return _get_export_stmt_part($db,undef,$alias,$joins,$conditions);
    
}

sub _get_export_stmt_part {

    my ($db,$static_context,$alias,$joins,$conditions,$order_by_id) = @_;

    my $table = $db->tableidentifier($tablename);

    my $table_aliased = $table;
    $table_aliased .= ' AS ' . $alias if $alias;
    my $stmt = "FROM " . $table_aliased;
    my @intjoins = ();
    if (defined $joins and (scalar @$joins) > 0) {
        foreach my $f (@$joins) {
            my ($tbl, $keys) = %{ $f };
            my $j = "LEFT JOIN $tbl";
            if ($keys) {
                my ($foreign_key, $own_key) = %{ $keys };
                $j .= " ON $foreign_key = $own_key";
            }
            push @intjoins, $j;
        }
    }

    my $order_by = ' ORDER BY ' . ($alias ? $alias : $table) . '.id';
    my @conds = ();
    if (defined $static_context and $static_context->{export_status_id} and $static_context->{part}) {
        unless (defined $static_context->{last_processed_cdr_id}) {
            $static_context->{last_processed_cdr_id} = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::find_last_processed_cdrid($static_context->{export_status_id});
        }
        if ('b' eq lc($static_context->{part})) {
            push @conds, ($alias ? $alias : $table) . '.id > ' . $static_context->{last_processed_cdr_id};
        } elsif ('a' eq lc($static_context->{part}))  {
            $stmt = "FROM " . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::gettablename())
                . ' AS __cesd FORCE INDEX (PRIMARY)';
            unshift @intjoins, 'LEFT JOIN ' . $table_aliased . ' FORCE INDEX (PRIMARY) ON ' . ($alias ? $alias : $table) . '.id = __cesd.cdr_id';
            push @conds, '__cesd.cdr_id <= ' . $static_context->{last_processed_cdr_id};
            push @conds, '__cesd.status_id = ' . $static_context->{export_status_id};
            push @conds, '__cesd.export_status = "' . $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::UNEXPORTED . '"';
            $order_by = ' ORDER BY __cesd.cdr_id';
        }
    }
    
    $stmt .= " " . join(" ", @intjoins) if (scalar @intjoins) > 0;
    if (defined $conditions and (scalar @$conditions) > 0) {
        foreach my $f (@$conditions) {
            my ($field, $match) = %{ $f };
            my ($op, $val);
            if (ref $match) {
                ($op, $val) = %{ $match };
            } else {
                $op = $match;
            }
            my $c = "$field $op";
            $c .= " $val" if defined $val;
            push @conds, $c;
        }
    }
    $stmt .= " WHERE " . join(" AND ", @conds) if (scalar @conds) > 0;
    $stmt .= $order_by if $order_by_id;
    return $stmt;

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...
            $record->load_relation($load_recursive,'source_user','NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_uuid',$record->{source_user_id},$load_recursive) if $record->{source_user_id};
            $record->load_relation($load_recursive,'source_provider','NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id',$record->{source_provider_id},$load_recursive) if $record->{source_provider_id};
            $record->load_relation($load_recursive,'source_account','NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id',$record->{source_account_id},$load_recursive) if $record->{source_account_id};

            $record->load_relation($load_recursive,'destination_user','NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_uuid',$record->{destination_user_id},$load_recursive) if $record->{destination_user_id};
            $record->load_relation($load_recursive,'destination_provider','NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id',$record->{destination_provider_id},$load_recursive) if $record->{destination_provider_id};
            $record->load_relation($load_recursive,'destination_account','NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id',$record->{destination_account_id},$load_recursive) if $record->{destination_account_id};

            $record->load_relation($load_recursive,'source_carrier_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{source_carrier_billing_fee_id},$load_recursive) if $record->{source_carrier_billing_fee_id};
            $record->load_relation($load_recursive,'source_reseller_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{source_reseller_billing_fee_id},$load_recursive) if $record->{source_reseller_billing_fee_id};
            $record->load_relation($load_recursive,'source_customer_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{source_customer_billing_fee_id},$load_recursive) if $record->{source_customer_billing_fee_id};
            $record->load_relation($load_recursive,'source_carrier_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{source_carrier_billing_zone_id},$load_recursive) if $record->{source_carrier_billing_zone_id};
            $record->load_relation($load_recursive,'source_reseller_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{source_reseller_billing_zone_id},$load_recursive) if $record->{source_reseller_billing_zone_id};
            $record->load_relation($load_recursive,'source_customer_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{source_customer_billing_zone_id},$load_recursive) if $record->{source_customer_billing_zone_id};

            $record->load_relation($load_recursive,'destination_carrier_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{destination_carrier_billing_fee_id},$load_recursive) if $record->{destination_carrier_billing_fee_id};
            $record->load_relation($load_recursive,'destination_reseller_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{destination_reseller_billing_fee_id},$load_recursive) if $record->{destination_reseller_billing_fee_id};
            $record->load_relation($load_recursive,'destination_customer_billing_fee','NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees_history::findby_id',$record->{destination_customer_billing_fee_id},$load_recursive) if $record->{destination_customer_billing_fee_id};
            $record->load_relation($load_recursive,'destination_carrier_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{destination_carrier_billing_zone_id},$load_recursive) if $record->{destination_carrier_billing_zone_id};
            $record->load_relation($load_recursive,'destination_reseller_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{destination_reseller_billing_zone_id},$load_recursive) if $record->{destination_reseller_billing_zone_id};
            $record->load_relation($load_recursive,'destination_customer_billing_zone','NGCP::BulkProcessor::Dao::Trunk::billing::billing_zones_history::findby_id',$record->{destination_customer_billing_zone_id},$load_recursive) if $record->{destination_customer_billing_zone_id};

            $record->load_relation($load_recursive,'cdr_groups','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_group::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_tags','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_tag_data::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_mos','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_mos_data::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_status','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_relations','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_relation_data::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_presentity','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_presentity::findby_callid',undef,$record->{call_id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_cash_balances','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_cash_balance_data::findby_cdrid',undef,$record->{id},$load_recursive);
            $record->load_relation($load_recursive,'cdr_time_balances','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_time_balance_data::findby_cdrid',undef,$record->{id},$load_recursive);

            push @records,$record;
        }
    }

    return \@records;

}

sub is_xfer {
    my $self = shift;
    if (length($self->{call_id}) > length($XFERSUFFIX)
        and substr($self->{call_id},-1 * length($XFERSUFFIX)) eq $XFERSUFFIX) {
        return 1;
    }
    return 0;
}

sub is_pbx {
    my $self = shift;
    if (length($self->{call_id}) > length($PBXSUFFIX)
        and substr($self->{call_id},-1 * length($PBXSUFFIX)) eq $PBXSUFFIX) {
        return 1;
    }
    return 0;
}

sub gettablename {

    return $tablename;

}

sub settablename {
    
    $tablename = shift;
    
}

sub check_table {

    return checktableinfo($get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
