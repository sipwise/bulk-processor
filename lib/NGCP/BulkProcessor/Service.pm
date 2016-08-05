package NGCP::BulkProcessor::Service;
use strict;

## no critic

use threads qw(yield);
use threads::shared;

use NGCP::BulkProcessor::Globals qw(
    @jobservers
    $jobnamespace
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    servicedebug
    serviceinfo
);
use NGCP::BulkProcessor::LogError qw(
    serviceerror
    servicewarn
    notimplementederror
);

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Serialization qw(serialize deserialize);

use Encode qw(encode_utf8);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(); #get_tableidentifier);

#my $logger = getlogger(__PACKAGE__);

use Gearman::Worker;
#use Time::HiRes qw(usleep);
use Time::HiRes qw(sleep);

my $sleep_secs_default = 0.1; # = 0.1;

my $instance_counts = {};

sub new {

    my $base_class = shift;
    my $class = shift;
    #my $self = bless {}, $class;
    my ($functions,$serialization_format,$no_autostart) = @_;
    my $self = bless {}, $class;

    $self->{worker} = undef;
    $self->{functions} = $functions;
    #$self->_register_functions();

    my $running = 0;
    $self->{running_ref} = share($running);
    $self->{thread} = undef;
    $self->{create_tid} = threadid();
    $self->{tid} = $self->{create_tid};
    $self->{worker_tid} = undef;
    $self->{sleep_secs} = $sleep_secs_default;
    $self->{serialization_format} = $serialization_format;

    my $identifier = ref $self;
    my $instance_count;
    if (exists $instance_counts->{$identifier}) {
        $instance_count = $instance_counts->{$identifier};
    } else {
        $instance_count = 0;
    }
    $self->{instance} = $instance_count;
    $instance_count++;
    $instance_counts->{$identifier} = $instance_count;

    if (not $no_autostart) {
        $self->start();
    }

    #$self = share($self);
    #autostart??
    servicedebug($self,$class . ' service created',getlogger(__PACKAGE__));

    return $self;

}

sub identifier {
    my $self = shift;
    return (ref $self) . '(' . $self->{instance} . ')';
}

sub _register_functions {

    my $self = shift;
    my $functions = $self->{functions};

    if (defined $functions and ref $functions eq 'HASH') {
        my $count = 0;
        foreach my $name (keys %$functions) {
            my $code = $functions->{$name};
            if (defined $code and ref $code eq 'CODE') {
                $self->{worker}->register_function($name,
                    sub {
                        #servicedebug($self,(ref $self) . ' connector destroyed',getlogger(__PACKAGE__));
                        #my $resultref = serialize(&$code(deserialize($_[0]->argref())));
                        #servicedebug($self,(ref $self) . ' connector destroyed',getlogger(__PACKAGE__));
                        servicedebug($self,'invoking \'' . $name . '\', args length: ' . length(encode_utf8($_[0]->arg())),getlogger(__PACKAGE__));
                        my $arg = deserialize($_[0]->arg(),$self->{serialization_format});
                        my (@ret) = &$code(@$arg);
                        my $result = serialize(\@ret,$self->{serialization_format});
                        servicedebug($self,'returning from \'' . $name . '\', result length: ' . length(encode_utf8($result)),getlogger(__PACKAGE__));
                        #$_[0]->set_status($numerator, $denominator);???
                        return $result;
                    }
                );
                servicedebug($self,'function \'' . $name . '\' registered',getlogger(__PACKAGE__));
                $count++;
            } else {
                servicewarn($self,'cannot register function ' . $name,getlogger(__PACKAGE__));
            }
        }
        serviceinfo($self,$count . ' functions registered at job servers ' . join(',',@jobservers),getlogger(__PACKAGE__));
    } else {
        serviceerror($self,'no functions to register',getlogger(__PACKAGE__));
    }

}

sub _unregister_functions {

    my $self = shift;
    my $functions = $self->{functions};

    if (defined $functions and ref $functions eq 'HASH') {
        my $count = 0;
        foreach my $name (keys %$functions) {
            $self->{worker}->unregister_function($name);
            servicedebug($self,'function \'' . $name . '\' unregistered',getlogger(__PACKAGE__));
            $count++;
        }
        serviceinfo($self,$count . ' functions unregistered from job servers ' . join(',',@jobservers),getlogger(__PACKAGE__));
    } else {
        serviceerror($self,'no functions to unregister',getlogger(__PACKAGE__));
    }

}

sub _worker {

    my $context = shift;
    #my $tid = threadid();
    my $service = $context->{service};
    #${$context->{service}->{tid_ref}} = $tid;
    $service->{worker_tid} = threadid();
    $service->{tid} = $service->{worker_tid};
    servicedebug($service,'worker thread ' . $service->{worker_tid} . ' started',getlogger(__PACKAGE__));
    my $running_ref = $service->{running_ref};

    my $stop_if = sub {
                        lock($running_ref);
                        #print join ',',@_ . "\n";
                        if (not $$running_ref) {
                            servicedebug($service,'shutting down work and worker thread ' . $service->{worker_tid} . ' ...',getlogger(__PACKAGE__));
                            return 1;
                        } else {
                            return 0;
                        }
                    };

    my %worker_opts = (on_start => sub { $service->_on_start(@_); },
                       on_complete => sub { $service->_on_complete(@_); },
                       on_fail => sub { $service->_on_fail(@_); },
                       stop_if => $stop_if );

    $service->{worker} = Gearman::Worker->new(( job_servers => \@jobservers,
                                             prefix => $jobnamespace));
    $service->_register_functions();

    while (not &$stop_if()) {
        $service->{worker}->work(%worker_opts);
        if ($service->{sleep_secs} > 0) {
            sleep($service->{sleep_secs});
        } else {
            yield();
        }
    }
    $service->_unregister_functions();
    #servicedebug($service,'worker thread ' . $service->{worker_tid} . ' shutting down',getlogger(__PACKAGE__));
    #threads->exit();
}

sub start {

    my $self = shift;
    if ($self->_is_create_thread()) {
        my $running_ref = $self->{running_ref};
        my $startup = 0;
        {
            lock($running_ref);
            if (not $$running_ref) {
                $$running_ref = 1;
                $startup = 1;
            }
        }
        if ($startup) {
            servicedebug($self,'starting worker thread ...',getlogger(__PACKAGE__));
            $self->{thread} = threads->create(\&_worker,

                                              { service                => $self,
                                                #logger               => $logger,
                                              }

                                              );
            #$self->{worker_tid} = $self->{thread}->tid();
        } else {
            servicewarn($self,'worker thread already running?',getlogger(__PACKAGE__));
        }

    }

}

sub stop {

    my $self = shift;
    if ($self->_is_create_thread()) {
        my $running_ref = $self->{running_ref};
        my $shutdown = 0;
        {
            lock($running_ref);
            if ($$running_ref) {
                $$running_ref = 0;
                $shutdown = 1;
            }
        }
        if ($shutdown) {
            servicedebug($self,'stopping worker thread ...',getlogger(__PACKAGE__));
            $self->{thread}->join();
            $self->{thread} = undef;
            $self->{worker_tid} = undef;
            servicedebug($self,'worker thread joined',getlogger(__PACKAGE__));
        } else {
            servicewarn($self,'thread already stopped',getlogger(__PACKAGE__));
        }
    }

}

sub _on_start {
    my $self = shift;
    if ($self->_is_worker_thread()) {
        servicedebug($self,'on_start',getlogger(__PACKAGE__));
    }
}

sub _on_complete {
    my $self = shift;
    if ($self->_is_worker_thread()) {
        servicedebug($self,'on_complete',getlogger(__PACKAGE__));
    }
}

sub _on_fail {
    my $self = shift;
    if ($self->_is_worker_thread()) {
        servicedebug($self,'on_fail',getlogger(__PACKAGE__));
    }
}

sub DESTROY {

    my $self = shift;
    #print "DESTROY is worker: " . $self->_is_worker_thread() . "\n";
    if ($self->_is_create_thread()) {
        servicedebug($self,'destroying service ...',getlogger(__PACKAGE__));
        $self->stop();
        servicedebug($self,(ref $self) . ' service destroyed',getlogger(__PACKAGE__));
    }

}

sub _is_worker_thread {
    my $self = shift;
    return (defined $self->{worker_tid} and $self->{worker_tid} == threadid());
}

sub _is_create_thread {
    my $self = shift;
    return $self->{create_tid} == threadid();
}

1;
