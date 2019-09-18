package NGCP::BulkProcessor::Dao::Trunk::billing::billing_fees;
use strict;

## no critic

use threads::shared;

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

#use NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table


    source_process_records

);

my $tablename = 'billing_fees';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
  'id',
  'billing_profile_id',
  'billing_zone_id',
  'source',
  'destination',
  'direction',
  'type',
  'onpeak_init_rate',
  'onpeak_init_interval',
  'onpeak_follow_rate',
  'onpeak_follow_interval',
  'offpeak_init_rate',
  'offpeak_init_interval',
  'offpeak_follow_rate',
  'offpeak_follow_interval',
  'use_free_time',
  #'match_mode',
];

my $indexes = {};

my $insert_unique_fields = [];

#enum('regex_longest_pattern','regex_longest_match','prefix','exact_destination')

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

sub source_process_records {

    my %params = @_;
    my ($source_dbs,
        $process_code,
        $read_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads) = @params{qw/
            source_dbs
            process_code
            read_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
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
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        'select'                    => 'SELECT * FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"',
        'selectcount'               => 'SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"',
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

            $record->{billing_mappings} = NGCP::BulkProcessor::Dao::mr341::billing::billing_mappings::source_findby_contractid($source_dbs,$record->{id});
            $record->{contact} = NGCP::BulkProcessor::Dao::mr341::billing::contacts::source_findby_id($source_dbs,$record->{contact_id});
            $record->{contract_balances} = NGCP::BulkProcessor::Dao::mr341::billing::contract_balances::source_findby_contractid($source_dbs,$record->{id});
            #contract_fraud_preferences
            if ($record->{contact}->{reseller_id}) {
                $record->{voip_subscribers} = NGCP::BulkProcessor::Dao::mr341::billing::voip_subscribers::source_findby_contractid($source_dbs,$record->{id});
            }

            push @records,$record;
        }
    }

    return \@records;

}

1;
