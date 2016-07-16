package NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances;
use strict;

## no critic

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

#my $logger = getlogger(__PACKAGE__);

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
#    'balance_interval' => [ 'contract_id','start','end' ],
#    'invoice_idx' => [ 'invoice_id' ],
#};

my $insert_unique_fields = []; #[ 'contract_id','start','end' ];

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

    my ($data,$insert_ignore) = @_;
    check_table();
    return insert_record($get_db,$tablename,$data,$insert_ignore,$insert_unique_fields);

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
