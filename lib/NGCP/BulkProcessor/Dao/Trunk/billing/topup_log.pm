package NGCP::BulkProcessor::Dao::Trunk::billing::topup_log;
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
    copy_row

    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    insert_row

    findby_contractidfromto
    findby_contractbalanceid
    findby_id

    $OK_OUTCOME
    $FAILED_OUTCOME

    $VOUCHER_TYPE
    $CASH_TYPE
    $SET_BALANCE_TYPE
);

my $tablename = 'topup_log';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
  'id',
  'username',
  'timestamp',
  'type',
  'outcome',
  'message',
  'subscriber_id',
  'contract_id',
  'amount',
  'voucher_id',
  'cash_balance_before',
  'cash_balance_after',
  'package_before_id',
  'package_after_id',
  'profile_before_id',
  'profile_after_id',
  'lock_level_before',
  'lock_level_after',
  'contract_balance_before_id',
  'contract_balance_after_id',
  'request_token',
];

my $indexes = {};

my $insert_unique_fields = [];

our $OK_OUTCOME = 'ok';
our $FAILED_OUTCOME = 'failed';

our $VOUCHER_TYPE = 'voucher';
our $CASH_TYPE = 'cash';
our $SET_BALANCE_TYPE = 'set_balance';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_contractidfromto {

    my ($contract_id,$from,$to,$outcome,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contract_id') . ' = ?';
    my @params = ($contract_id);
    if ($from) {
        $stmt .= ' AND ' . $db->columnidentifier('timestamp') . ' >= ?';
        push(@params,$from->epoch());
    }
    if ($to) {
        $stmt .= ' AND ' . $db->columnidentifier('timestamp') . ' <= ?';
        push(@params,$to->epoch());
    }
    if ($outcome) {
        $stmt .= ' AND ' . $db->columnidentifier('outcome') . ' = ?';
        push(@params,$outcome);
    }
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_contractbalanceid {

    my ($id,$outcome,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contract_balance_before_id') . ' = ?';
    my @params = ($id);
    if ($outcome) {
        $stmt .= ' AND ' . $db->columnidentifier('outcome') . ' = ?';
        push(@params,$outcome);
    }
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
        my ($contract_id) = @params{qw/
                contract_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('timestamp') . ', ' .
                $db->columnidentifier('type') . ', ' .
                $db->columnidentifier('outcome') .', ' .
                $db->columnidentifier('contract_id') . ') VALUES (' .
                'UNIX_TIMESTAMP(NOW()), ' .
                '\'' . $SET_BALANCE_TYPE . '\', ' .
                '\'' . $FAILED_OUTCOME . '\', ' .
                '?)',
                $contract_id
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
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
