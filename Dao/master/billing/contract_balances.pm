
package billing::contract_balances;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Logging qw(getlogger);

use ConnectorPool qw(get_billing_db
                    billing_db_tableidentifier);

use SqlRecord qw(checktableinfo);

require Exporter;
our @ISA = qw(Exporter SqlRecord);
our @EXPORT_OK = qw(
XX
backoffice_client_byboclientid
                        sync_table
                        drop_table

                        check_local_table
                        check_source_table);

my $logger = getlogger(__PACKAGE__);

my $tablename = 'contract_balances';
my $get_db = \&get_billing_db;
my $get_tablename = \&billing_db_tableidentifier;

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

sub new {

    my $class = shift;
    my $self = SqlRecord->new($get_db,
                           gettablename(),
                           $expected_fieldnames);

    bless($self,$class);

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

    my $db = &$get_local_db();
    return &$get_tablename($db,$tablename);

}

sub check_table {

    my $db = &$get_local_db();

    return checktableinfo($get_db,
                   gettablename(),
                   $expected_fieldnames);

}

1;