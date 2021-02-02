package NGCP::BulkProcessor::NoSqlConnectors::Redis;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $system_abbreviation
    $enablemultithreading
    $local_fqdn);
use NGCP::BulkProcessor::Logging qw(
    getlogger
    nosqlinfo
    nosqldebug);
use NGCP::BulkProcessor::LogError qw(nosqlerror nosqlwarn);

use Redis;

#use NGCP::BulkProcessor::Array qw(arrayeq itemcount contains setcontains);

use NGCP::BulkProcessor::NoSqlConnector qw(
    _share_scalar
    _share_list
);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::NoSqlConnector);
our @EXPORT_OK = qw(
    get_scan_args
);

our $AUTOLOAD;

my $log_operations = 0;

my $defaulthost = '127.0.0.1';
my $defaultport = '6379';
my $defaultsock = undef;
my $defaultpassword = undef;
my $defaultdatabaseindex = '0';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::NoSqlConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{sock} = undef;
    $self->{databaseindex} = undef;
    $self->{password} = undef;
    
    $self->{redis} = undef;

    bless($self,$class);

    nosqldebug($self,__PACKAGE__ . ' connector created',getlogger(__PACKAGE__));

    return $self;

}

sub connectidentifier {

    my $self = shift;
    if (defined $self->{databaseindex}) {
        if ($self->{sock}) {
            return $self->{sock} . '/' . $self->{databaseindex};
        } else {
            return $self->{host} . ':' . $self->{port} . '/' . $self->{databaseindex};
        }
    } else {
        return undef;
    }

}

sub connect {

    my $self = shift;

    my ($databaseindex,$password,$host,$port,$sock) = @_;
    
    $self->disconnect();

    $host = $defaulthost if (not $host);
    $port = $defaultport if (not $port);
    $sock = $defaultsock if (not $sock);
    $databaseindex = $defaultdatabaseindex if (not $databaseindex);
    $password = $defaultpassword if (not $password);

    $self->{host} = $host;
    $self->{port} = $port;
    $self->{sock} = $sock;
    $self->{databaseindex} = $databaseindex;
    $self->{password} = $password;

    #if (not contains($databasename,$self->getdatabases(),0)) {
    #    $self->_createdatabase($databasename);
    #}

    nosqldebug($self,'connecting',getlogger(__PACKAGE__));

    my $name = $system_abbreviation;
    $name .= '_' . $self->instanceidentifier() if $self->instanceidentifier();
    $name =~ s/[^a-z0-9]+/_/gi;
    $name =~ s/(^_+)|(_+$)//g;
    if ($enablemultithreading) {
        $name .= '_thread_' . $self->{tid};
    }

    my $redis = Redis->new(
        ($sock ?
            (sock => $sock) :
            (server => ($host . ':' . $port))
        ),
        (defined $password ? (password => $password) : ()),
        name => $name,
        no_auto_connect_on_new => 1,
        #debug => 1,
    );
    
    eval {
        $redis->connect();
        $redis->select($databaseindex);
        #or die($!);
        #my $dbsize = $redis->dbsize();
    };
    if ($@) {
        nosqlerror($self, 'error connecting: ' . $@, getlogger(__PACKAGE__));
    } else {
        $self->{redis} = $redis;
        nosqlinfo($self,'connected',getlogger(__PACKAGE__));
    }

}

sub disconnect {

    my $self = shift;

    # since this is also called from DESTROY, no die() here!

    if (defined $self->{redis}) {
        
        nosqldebug($self,'disconnecting',getlogger(__PACKAGE__));
        
        #$self->{redis}->wait_all_responses; #already part of quit()
        $self->{redis}->quit() or nosqlwarn($self,'error disconnecting: ' . $!,getlogger(__PACKAGE__));
        $self->{redis} = undef;

        nosqlinfo($self,'disconnected',getlogger(__PACKAGE__));

    }
    
}

sub is_connected {

    my $self = shift;
    return (defined $self->{redis});

}

sub ping {

    my $self = shift;

    return $self->{redis}->ping();

}

sub AUTOLOAD {
    
    my $self = shift;
    my @args = @_;
    
    my $called = $AUTOLOAD;
    my $regex = quotemeta(__PACKAGE__ . '::');
    $called =~ s/^$regex//;
    my $shared = 0;
    if ($called =~ /_shared$/) {
        $called = substr($called, 0, length($called) - length('_shared'));
        $shared = 1;
        if (ref $args[-1] eq 'CODE') {
            my $cb = pop @args;
            push(@args, sub {
                return $cb->(_share_list(@_));
            });
        }
    }
    
    nosqldebug($self, $called . '(' . join(', ', @args) . ')', getlogger(__PACKAGE__)) if $log_operations;
    
    if (wantarray) {
        my @result;
        eval {
            @result = $self->{redis}->$called(@args);
        };
        if ($@) {
            nosqlerror($self, $called . '(' . join(', ', @args) . ') error: ' . $@, getlogger(__PACKAGE__));
        }
        if ($shared) {
            @result = _share_list(@result);
        }
        return @result;
    } else {
        my $result;
        eval {
            $result = $self->{redis}->$called(@args);
        };
        if ($@) {
            nosqlerror($self, $called . '(' . join(', ', @args) . ') error: ' . $@, getlogger(__PACKAGE__));
        }
        if ($shared) {
            $result = _share_scalar($result);
        }
        return $result;
    }
    
}

sub multithreading_supported {

    my $self = shift;
    return 1;

}

sub get_scan_args {
    
    my ($scan_pattern,$blocksize,$type) = @_;
    my @result = ();
    if ($scan_pattern) {
        push(@result,'MATCH');
        push(@result,$scan_pattern);
    }
    if ($blocksize) {
        push(@result,'COUNT');
        push(@result,$blocksize);
    }
    # As of version 6.0 you can use this option to ask SCAN to only return objects that match a given type:
    #if ($type) {
    #    push(@result,'TYPE'); 
    #    push(@result,$type);
    #}
    return @result;
    
}

1;
