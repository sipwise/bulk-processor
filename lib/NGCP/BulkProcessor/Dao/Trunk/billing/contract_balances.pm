package NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
);

my $tablename = 'contract_balances';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'cash_balance',
    'cash_balance_interval',
    'free_time_balance',
    'free_time_balance_interval',
    'topup_count',
    'timely_topup_count',
    'start',
    'end',
    'invoice_id',
    'underrun_profiles',
    'underrun_lock',
];

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,$tablename,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($contract_id) = @params{qw/
                contract_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('cash_balance') . ', ' .
                $db->columnidentifier('cash_balance_interval') . ', ' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('end') . ', ' .
                $db->columnidentifier('free_time_balance') . ', ' .
                $db->columnidentifier('free_time_balance_interval') . ', ' .
                $db->columnidentifier('start') . ', ' .
                $db->columnidentifier('underrun_lock') . ', ' .
                $db->columnidentifier('underrun_profiles') . ') VALUES (' .
                '0.0, ' .
                '0.0, ' .
                '?, ' .
                'CONCAT(LAST_DAY(NOW()),\' 23:59:59\'), ' .
                '0, ' .
                '0, ' .
                'CONCAT(SUBDATE(CURDATE(),(DAY(CURDATE())-1)),\' 00:00:00\'), ' .
                'NULL, ' .
                'NULL)',
                $contract_id,
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
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
