package Service::TestService;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Logging qw(getlogger servicedebug);

use Service;

#use test::csv_table; # qw(test_table_bycolumn1);
#use test::mysql_table;
#use test::oracle_table;
#use test::postgres_table;
#use test::sqlite_table;
#use test::sqlserver_table;

use Utils; # qw(create_guid);

require Exporter;
our @ISA = qw(Exporter Service);
our @EXPORT_OK = qw(
    roundtrip
    sleep_seconds
    noop
    exception
);

#my $logger = getlogger(__PACKAGE__);

my $functions = {
    create_uuid => \&Utils::create_guid,
    roundtrip => \&roundtrip,
    noop => \&noop,
    exception => \&exception,
    sleeproundtrip => \&sleep_roundtrip,
    #test_csv_table_bycolumn1 => \&test::csv_table::test_table_bycolumn1,
    #test_mysql_table_bycolumn1 => \&test::mysql_table::test_table_bycolumn1,
    #test_oracle_table_bycolumn1 => \&test::oracle_table::test_table_bycolumn1,
    #test_postgres_table_bycolumn1 => \&test::postgres_table::test_table_bycolumn1,
    #test_sqlite_table_bycolumn1 => \&test::sqlite_table::test_table_bycolumn1
};

sub new {

    #my $class = shift;
    #my $self = Service->new($functions,$class);

    #bless($self,$class);

    #return $self;

    my $self = Service->new($functions,@_);
    servicedebug($self,__PACKAGE__ . ' service created',getlogger(__PACKAGE__));
    return $self;

}

sub roundtrip {
    return @_;
    #my (@in) = @_;
    ##my $error = 1/0;
    #return @in;
}

sub sleep_roundtrip {
    sleep(shift);
    return @_;
}

sub noop {

}

sub exception {
    return 1/0;
}

#sub _on_start {
#    my $self = shift;
#    print "_on_start\n";
#}

#sub _on_complete {
#    my $self = shift;
#    print "_on_complete\n";
#}

#sub _on_fail {
#    my $self = shift;
#    print "_on_fail\n";
#}

1;