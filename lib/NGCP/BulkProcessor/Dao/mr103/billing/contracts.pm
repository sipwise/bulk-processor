package NGCP::BulkProcessor::Dao::mr103::billing::contracts;
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

use NGCP::BulkProcessor::Dao::mr103::billing::voip_subscribers qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row

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
    'reseller_id',
    'contact_id',
    'order_id',
    'status',
    'modify_timestamp',
    'create_timestamp',
    'activate_timestamp',
    'terminate_timestamp',
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
        blocksize                   => $blocksize,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => $destroy_reader_dbs_code,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        'select'                    => 'SELECT * FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"', # and id = 7185',
        'selectcount'               => 'SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"', # and id = 7185',
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

            #$record->{billing_mappings} = NGCP::BulkProcessor::Dao::mr103::billing::billing_mappings::source_findby_contractid($source_dbs,$record->{id});
            #$record->{contact} = NGCP::BulkProcessor::Dao::mr103::billing::contacts::source_findby_id($source_dbs,$record->{contact_id});
            #$record->{contract_balances} = NGCP::BulkProcessor::Dao::mr103::billing::contract_balances::source_findby_contractid($source_dbs,$record->{id});
            #if ($record->{reseller_id}) {
                $record->{voip_subscribers} = NGCP::BulkProcessor::Dao::mr103::billing::voip_subscribers::source_findby_contractid($source_dbs,$record->{id});
            #}

            push @records,$record;
        }
    }

    return \@records;

}

1;
