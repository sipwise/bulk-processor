package NGCP::BulkProcessor::Dao::Trunk::billing::contracts;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    copy_row

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row

    countby_status_resellerid
    findby_contactid
    findby_id
    forupdate_id

    process_records
    process_free_cash_contracts

    countby_free_cash

    $ACTIVE_STATE
    $TERMINATED_STATE
);

my $tablename = 'contracts';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'customer_id',
    'contact_id',
    'order_id',
    'profile_package_id',
    'status',
    'external_id',
    'modify_timestamp',
    'create_timestamp',
    'activate_timestamp',
    'terminate_timestamp',
    'max_subscribers',
    'send_invoice',
    'subscriber_email_template_id',
    'passreset_email_template_id',
    'invoice_email_template_id',
    'invoice_template_id',
    'vat_rate',
    'add_vat',
];

my $indexes = {};

my $insert_unique_fields = [];

our $ACTIVE_STATE = 'active';
our $TERMINATED_STATE = 'terminated';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_contactid {

    my ($contact_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contact_id') . ' = ?';
    my @params = ($contact_id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

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

sub forupdate_id {

    my ($xa_db,$id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ? FOR UPDATE';
    my @params = ($id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub countby_status_resellerid {

    my ($status,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' AS contract' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contacts::gettablename()) . ' AS contact ON contract.contact_id = contact.id';
    my @params = ();
    my @terms = ();
    if ($status) {
        push(@terms,'contract.status = ?');
        push(@params,$status);
    }
    if ($reseller_id) {
        if ('ARRAY' eq ref $reseller_id) {
            push(@terms,'contact.reseller_id IN (' . substr(',?' x scalar @$reseller_id,1) . ')');
            push(@params,@$reseller_id);
        } else {
            push(@terms,'contact.reseller_id = ?');
            push(@params,$reseller_id);
        }
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub countby_free_cash {

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(DISTINCT c.id) FROM ' . $table . ' AS c' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::gettablename()) . ' AS bm ON bm.contract_id = c.id' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::gettablename()) . ' AS bp ON bp.id = bm.billing_profile_id' .
    ' WHERE c.status != "terminated" AND bp.interval_free_cash <> 0.0';

    return $db->db_get_value($stmt);

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
        my ($contact_id) = @params{qw/
                contact_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('contact_id') . ', ' .
                $db->columnidentifier('create_timestamp') . ', ' .
                $db->columnidentifier('modify_timestamp') . ', ' .
                $db->columnidentifier('status') . ') VALUES (' .
                '?, ' .
                'NOW(), ' .
                'NOW(), ' .
                '\'' . $ACTIVE_STATE . '\')',
                $contact_id,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

}

sub process_records {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            load_recursive
        /};

    check_table();

    return process_table(
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,buildrecords_fromrows($rowblock,$load_recursive),$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
    );
}

sub process_free_cash_contracts {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
        /};

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'FROM ' . $table . ' AS c' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::gettablename()) . ' AS bm ON bm.contract_id = c.id' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::gettablename()) . ' AS bp ON bp.id = bm.billing_profile_id' .
    ' WHERE c.status != "terminated" AND bp.interval_free_cash <> 0.0';

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
        select                      => $db->paginate_sort_query("SELECT DISTINCT c.id " . $stmt,undef,undef,[{ column => 'c.id', numeric => 1, dir => 1 }]),
        selectcount                 => "SELECT COUNT(DISTINCT c.id) " . $stmt,
    );
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
