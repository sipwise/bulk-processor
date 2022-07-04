package NGCP::BulkProcessor::NoSqlConnector;
use strict;

## no critic

use threads;
use threads::shared;

use NGCP::BulkProcessor::Logging qw(
    getlogger
    nosqlinfo
    nosqldebug);
use NGCP::BulkProcessor::LogError qw(nosqlerror notimplementederror);

use NGCP::BulkProcessor::Utils qw(threadid);


require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    _share_scalar
    _share_list
);

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my $instanceid = shift;

    $self->{instanceid} = $instanceid;
    $self->{tid} = threadid();

    return $self;

}

sub connectidentifier {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub instanceidentifier {
    my $self = shift;

    $self->{instanceid} = shift if @_;
    return $self->{instanceid};

}

sub connect {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub disconnect {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub is_connected {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub ping {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub get_defaultblockcount {
    
    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    
}

sub multithreading_supported {

    my $self = shift;
    return 0;

}

sub DESTROY {

    my $self = shift;

    # perl threads works like a fork, each thread owns a shalow? copy
    # of the entire current context, at the moment it starts.
    # due to this, if the thread is finished, perl gc will invoke destructors
    # on the thread's scope elements, that potentially contains connectors from
    # the main tread. it will actually attempt destroy them (disconnect, etc.)
    # this is a problem with destructors that change object state like this one
    #
    # to avoid this, we perform destruction tasks only if the destructing tid
    # is the same as the creating one:

    if ($self->{tid} == threadid()) {
        $self->disconnect();
        eval {
            nosqldebug($self,(ref $self) . ' connector destroyed',getlogger(__PACKAGE__));
        };
    }

}

sub _share_list {
    my @args = @_;
    my $result = shared_clone(\@args);
    return @$result;
}

sub _share_scalar {
    my $result = shared_clone(shift @_);
    return $result;
}

1;
