package NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_time_balance_data;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row

    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_provider qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_direction qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    settablename
    check_table

    findby_cdrid
    
    insert_row
);

my $tablename = 'cdr_time_balance_data';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
  "cdr_id",
  "provider_id",
  "direction_id",
  "time_balance_id",
  "val_before",
  "val_after",  
  "cdr_start_time",
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

sub findby_cdrid {

    my ($xa_db,$cdrid,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('cdr_id') . ' = ?';
    my @params = ($cdrid);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;

    my ($data,$insert_ignore) = @_;
    check_table();
    if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
        return $xa_db->db_last_insert_id() || 1;
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
            $record->load_relation($load_recursive,'contract_balance','NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::findby_id',undef,$record->{time_balance_id},$load_recursive);
            $record->load_relation($load_recursive,'direction','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_direction::findby_id_cached',$record->{direction_id},$load_recursive);
            $record->load_relation($load_recursive,'provider','NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_provider::findby_id_cached',$record->{provider_id},$load_recursive);

            push @records,$record;
        }
    }

    return \@records;

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
