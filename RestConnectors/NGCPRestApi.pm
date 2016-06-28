package RestConnectors::NGCPRestApi;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Globals qw($LongReadLen_limit);
use Logging qw(
    getlogger
    restdebug
    restinfo
);
use LogError qw(
    resterror
    restwarn
    restrequesterror
    restresponseerror);

use RestConnector;

require Exporter;
our @ISA = qw(Exporter RestConnector);
our @EXPORT_OK = qw(get_tableidentifier);

my $defaulthost = '127.0.0.1';
my $defaultport = '3306';
my $defaultusername = 'root';
my $defaultpassword = '';
my $defaultdatabasename = 'test';

my $logger = getlogger(__PACKAGE__);

sub new {

    my $class = shift;

    my $self = RestConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{databasename} = undef;
    $self->{username} = undef;
    $self->{password} = undef;

    $self->{drh} = DBI->install_driver('mysql');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',$logger);

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    if (defined $self->{databasename}) {
        return $self->{username} . '@' . $self->{host} . ':' . $self->{port} . '.' . $self->{databasename};
    } else {
        return undef;
    }

}




1;