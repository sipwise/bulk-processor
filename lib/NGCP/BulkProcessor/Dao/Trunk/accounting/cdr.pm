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

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
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

my @callid_suffixes = ();
my $PBXSUFFIX = '_pbx-1';
push(@callid_suffixes,$PBXSUFFIX);
my $XFERSUFFIX = '_xfer-1';
push(@callid_suffixes,$XFERSUFFIX);

our $OK_CALL_STATUS = 'ok';

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

sub get_callidprefix {

    my ($call_id) = @_;
    my $suffixre = '(' . join('|', map { quotemeta($_); } @callid_suffixes) . ')+$';
    $call_id =~ s/$suffixre//g;
    return $call_id

}

sub findby_callidprefix {

    my ($xa_db,$call_id,$joins,$conditions,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    $call_id = get_callidprefix($call_id);
    $call_id =~ s/%/\\%/g;

    my @conditions = @{$conditions // []};
    push(@conditions,{ $table . '.call_id' => { 'LIKE' => '?' } });
    my $stmt = 'SELECT ' . join(',', map { $table . '.' . $db->columnidentifier($_); } @$expected_fieldnames) . ' ' .
        _get_export_stmt($db,undef,$joins,\@conditions) .
        ' ORDER BY LENGTH(' . $table . '.call_id' . ') ASC, ' . $table . '.start_time ASC';
    my @params = ($call_id . '%');
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_callid {

    my ($xa_db,$call_id,$joins,$conditions,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my @conditions = @{$conditions // []};
    push(@conditions,{ $table . '.call_id' => { '=' => '?' } });
    my $stmt = 'SELECT ' . join(',', map { $table . '.' . $db->columnidentifier($_); } @$expected_fieldnames) . ' ' .
        _get_export_stmt($db,undef,$joins,\@conditions) .
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
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $blocksize,
        $joins,
        $conditions,
        #$sort,
        $limit) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            blocksize
            joins
            conditions
            limit
        /};
    #sort

    check_table();
    NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = _get_export_stmt($db,$static_context,$joins,$conditions);

    return process_table(
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
        #select                      => $db->paginate_sort_query('SELECT ' . $table . '.* ' . $stmt,undef,undef,$sort),
        select                      => 'SELECT ' . $table . '.' . $db->columnidentifier('id') . ', ' . $table . '.' . $db->columnidentifier('call_id') .
                                       ' ' . $stmt . ' ORDER BY ' . $table . '.' . $db->columnidentifier('id'),
        selectcount                 => 'SELECT COUNT(1) FROM (' . $db->paginate_sort_query('SELECT 1 ' . $stmt,0,$limit,undef) . ') AS __cnt',
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

    my ($db,$static_context,$joins,$conditions) = @_;

    my $table = $db->tableidentifier($tablename);

    my $stmt = "FROM " . $table;
    my @intjoins = ();
    if (defined $joins and (scalar @$joins) > 0) {
        foreach my $f (@$joins) {
            my ($table, $keys) = %{ $f };
            my ($foreign_key, $own_key) = %{ $keys };
            push @intjoins, "LEFT JOIN $table ON $foreign_key = $own_key";
        }
    }
    my @conds = ();
    if (defined $static_context and $static_context->{export_status_id}) {
        push @intjoins, 'LEFT JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::gettablename()) . ' AS __cesd ON __cesd.cdr_id = ' . $table . '.id AND __cesd.status_id = ' . $static_context->{export_status_id};
        push @conds, '(__cesd.export_status = "' . $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::UNEXPORTED . '" OR __cesd.export_status IS NULL)';
    }
    $stmt .= " " . join(" ", @intjoins) if (scalar @intjoins) > 0;
    if (defined $conditions and (scalar @$conditions) > 0) {
        foreach my $f (@$conditions) {
            my ($field, $match) = %{ $f };
            my ($op, $val) = %{ $match };
            push @conds, "$field $op $val";
        }
    }
    $stmt .= " WHERE " . join(" AND ", @conds) if (scalar @conds) > 0;
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

sub check_table {

    return checktableinfo($get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
