package NGCP::BulkProcessor::Dao::mr553::billing::contracts;
use strict;

use threads::shared;

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

use NGCP::BulkProcessor::Dao::mr553::billing::billing_mappings qw();
use NGCP::BulkProcessor::Dao::mr553::billing::contacts qw();
use NGCP::BulkProcessor::Dao::mr553::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::mr553::billing::voip_subscribers qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row

    countby_status_resellerid

    process_records
    source_process_records

    source_findby_id

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

sub countby_status_resellerid {

    my ($status,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' AS contract' .
    ' INNER JOIN billing.contact AS contact ON contract.contact_id = contact.id';
    my @params = ();
    my @terms = ();
    if ($status) {
        push(@terms,'contract.status = ?');
        push(@params,$status);
    }
    if ($reseller_id) {
        push(@terms,'contact.reseller_id = ?');
        push(@params,$reseller_id);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

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

sub source_process_records {

    my %params = @_;
    my ($source_dbs,
        $process_code,
        $read_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $destroy_reader_dbs_code,
        $multithreading,
        $blocksize,
        $numofthreads) = @params{qw/
            source_dbs
            process_code
            read_code
            static_context
            init_process_context_code
            uninit_process_context_code
            destroy_reader_dbs_code
            multithreading
            blocksize
            numofthreads
        /};

    my $source_db = $source_dbs->{billing_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    return process_table(
        get_db                      => $source_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                #return &$process_code($context,buildrecords_fromrows_source($rowblock,$source_db,$load_recursive),$row_offset);
                return &$process_code($context,$rowblock,$row_offset);
            },
        read_code                => sub {
                my ($rowblock) = @_;
                return source_buildrecords_fromrows($rowblock,$source_dbs);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => $destroy_reader_dbs_code,
        multithreading              => $multithreading,
        blocksize                   => $blocksize,
        tableprocessing_threads     => $numofthreads,
        'select'                    => 'SELECT * FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"', # and id=55319',
        'selectcount'               => 'SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"', # and id=55319',
    );
}

sub source_findby_id {

    my ($source_dbs,$id) = @_;

    my $source_db = $source_dbs->{billing_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';

    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs)->[0];

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{billing_db},$row);

            $record->{billing_mappings} = NGCP::BulkProcessor::Dao::mr553::billing::billing_mappings::source_findby_contractid($source_dbs,$record->{id});
            $record->{contact} = NGCP::BulkProcessor::Dao::mr553::billing::contacts::source_findby_id($source_dbs,$record->{contact_id});
            $record->{contract_balances} = NGCP::BulkProcessor::Dao::mr553::billing::contract_balances::source_findby_contractid($source_dbs,$record->{id});
            #contract_fraud_preferences
            #if ($record->{contact}->{reseller_id}) {
                $record->{voip_subscribers} = NGCP::BulkProcessor::Dao::mr553::billing::voip_subscribers::source_findby_contractid($source_dbs,$record->{id});
            #}

            push @records,$record;
        }
    }

    return \@records;

}

1;
