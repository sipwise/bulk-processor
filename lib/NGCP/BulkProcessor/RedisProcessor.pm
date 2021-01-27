package NGCP::BulkProcessor::RedisProcessor;
use strict;

## no critic

use Tie::IxHash;

use threads qw(yield);
use threads::shared;
use Thread::Queue;

use Time::HiRes qw(sleep);
#use URI::Escape qw();

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
    $cpucount
);
use NGCP::BulkProcessor::Logging qw(
    getlogger
    nosqlthreadingdebug
    nosqlprocessingstarted
    nosqlprocessingdone
    fetching_entries
    processing_entries
);

use NGCP::BulkProcessor::LogError qw(
    nosqlprocessingfailed
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    init_entry
    copy_value
    process_entries

);

my $nosqlprocessing_threadqueuelength = 10;

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;


sub init_entry {

    my ($entry,$fieldnames) = @_;
    
    if (defined $fieldnames) {
        # if there are fieldnames defined, we make a member variable for each and set it to undef
        foreach my $fieldname (@$fieldnames) {
            $entry->{value}->{$fieldname} = undef;
        }
    }

    return $entry;

}

sub copy_value {
    my ($entry,$value,$fieldnames) = @_;
    if (defined $entry) {
        if (defined $value) {
            if ($entry->{type} eq 'set') {
                if (ref $value eq 'ARRAY') {
                    %{$entry->{value}} = map { $_ => undef; } @$value;
                } elsif (ref $value eq 'HASH') {
                    %{$entry->{value}} = map { $_ => undef; } %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    %{$entry->{value}} = %{$value->{value}};
                } else {
                    $entry->{value} = { $value => undef, };
                }
            } elsif ($entry->{type} eq 'list') {
                if (ref $value eq 'ARRAY') {
                    @{$entry->{value}} = @$value;
                } elsif (ref $value eq 'HASH') {
                    @{$entry->{value}} = %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    @{$entry->{value}} = @{$value->{value}};
                } else {
                    $entry->{value} = [ $value, ];
                }                
            } elsif ($entry->{type} eq 'zset') {
                my %value = ();
                tie(%value, 'Tie::IxHash');
                $entry->{value} = \%value;
                if (ref $value eq 'ARRAY') {
                    map { $entry->{value}->Push($_ => undef); } @$value;
                } elsif (ref $value eq 'HASH') {
                    map { $entry->{value}->Push($_ => undef); } %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    map { $entry->{value}->Push($_ => undef); } keys %{$value->{value}};
                } else {
                    $entry->{value}->Push($value => undef);
                }
            } elsif ($entry->{type} eq 'hash') {
                my $i;
                if (ref $value eq 'ARRAY') {
                    $i = 0;
                } elsif (ref $value eq 'HASH') {
                    $i = -1;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    $i = -2;
                } else {
                    $i = -3;
                }
                foreach my $fieldname (@$fieldnames) {
                    if ($i >= 0) {
                        $entry->{value}->{$fieldname} = $value->[$i];
                        $i++;
                    } elsif ($i == -1) {
                        if (exists $value->{$fieldname}) {
                            $entry->{value}->{$fieldname} = $value->{$fieldname};
                        } elsif (exists $value->{uc($fieldname)}) {
                            $entry->{value}->{$fieldname} = $value->{uc($fieldname)};
                        } else {
                            $entry->{value}->{$fieldname} = undef;
                        }
                    } elsif ($i == -2) {
                        if (exists $value->{value}->{$fieldname}) {
                            $entry->{value}->{$fieldname} = $value->{value}->{$fieldname};
                        } elsif (exists $entry->{value}->{uc($fieldname)}) {
                            $entry->{value}->{$fieldname} = $value->{value}->{uc($fieldname)};
                        } else {
                            $entry->{value}->{$fieldname} = undef;
                        }                        
                    } else {
                        $entry->{value}->{$fieldname} = $value; #scalar
                        last;
                    }
                }
            } else { #($type eq 'string') {
                if (ref $value eq 'ARRAY') {
                    $entry->{value} = $value->[0];
                } elsif (ref $value eq 'HASH') {
                    my @keys = keys %$value; #Experimental shift on scalar is now forbidden at..
                    $entry->{value} = $value->{shift @keys};
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    $entry->{value} = $value->{value};
                } else {
                    $entry->{value} = $value;
                }                
            }
        }

    }
    return $entry;
}

sub process_entries {

    my %params = @_;
    my ($get_store,
        $scan_pattern,
        $process_code,
        $static_context,
        $blocksize,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $nosqlprocessing_threads) = @params{qw/
            get_store
            scan_pattern
            process_code
            static_context
            blocksize
            init_process_context_code
            uninit_process_context_code
            multithreading
            nosqlprocessing_threads
        /};

    if (ref $get_store eq 'CODE') {

        nosqlprocessingstarted(&$get_store(),$scan_pattern,getlogger(__PACKAGE__));

        my $errorstate = $RUNNING;
        my $tid = threadid();

        if ($enablemultithreading and $multithreading and $cpucount > 1) {

            $nosqlprocessing_threads //= $cpucount;

            my $reader;
            my %processors = ();
            my %errorstates :shared = ();
            my $queue = Thread::Queue->new();

            nosqlthreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            threadqueuelength    => $nosqlprocessing_threadqueuelength,
                                            get_store               => $get_store,
                                            scan_pattern            => $scan_pattern,
                                            blocksize            => $blocksize,
                                          });

            for (my $i = 0; $i < $nosqlprocessing_threads; $i++) {
                nosqlthreadingdebug('starting processor thread ' . ($i + 1) . ' of ' . $nosqlprocessing_threads,getlogger(__PACKAGE__));
                my $processor = threads->create(\&_process,
                                              _create_process_context($static_context,
                                              { queue                => $queue,
                                                errorstates          => \%errorstates,
                                                readertid              => $reader->tid(),
                                                process_code         => $process_code,
                                                init_process_context_code => $init_process_context_code,
                                                uninit_process_context_code => $uninit_process_context_code,
                                              }));
                if (!defined $processor) {
                    nosqlthreadingdebug('processor thread ' . ($i + 1) . ' of ' . $nosqlprocessing_threads . ' NOT started',getlogger(__PACKAGE__));
                }
                $processors{$processor->tid()} = $processor;
            }

            $reader->join();
            nosqlthreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            while ((scalar keys %processors) > 0) {
                foreach my $processor (values %processors) {
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        nosqlthreadingdebug('processor thread tid ' . $processor->tid() . ' joined',getlogger(__PACKAGE__));
                    }
                }
                sleep($thread_sleep_secs);
            }

            $errorstate = (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);

        } else {

            my $store = &$get_store(); #$reader_connection_name);
            $blocksize //= $store->get_defaultblockcount();

            my $context = _create_process_context($static_context,{ tid => $tid });
            my $rowblock_result = 1;
            my $blockcount = 0;
            eval {
                if (defined $init_process_context_code and 'CODE' eq ref $init_process_context_code) {
                    &$init_process_context_code($context);
                }

                my $i = 0;
                my $cursor = 0;
                while (1) {
                    fetching_entries($store,$scan_pattern,$i,$blocksize,getlogger(__PACKAGE__));
                    ($cursor, my $rowblock) = $store->scan($cursor,$scan_pattern,$blocksize);
                    my $realblocksize = scalar @$rowblock;
                    processing_entries($tid,$i,$realblocksize,getlogger(__PACKAGE__));
                    $rowblock_result = &$process_code($context,$rowblock,$i);

                    $i += $realblocksize;
                    $blockcount++;

                    last unless $rowblock_result;
                    last unless $cursor;
                }

            };

            if ($@) {
                $errorstate = $ERROR;
            } else {
                $errorstate = $COMPLETED; #(not $rowblock_result) ? $ERROR : $COMPLETED;
            }

            eval {
                if (defined $uninit_process_context_code and 'CODE' eq ref $uninit_process_context_code) {
                    &$uninit_process_context_code($context);
                }
            };

        }

        if ($errorstate == $COMPLETED) {
            nosqlprocessingdone(&$get_store(),$scan_pattern,getlogger(__PACKAGE__));
            return 1;
        } else {
            nosqlprocessingfailed(&$get_store(),$scan_pattern,getlogger(__PACKAGE__));
        }

    }

    return 0;

}


sub _reader {

    my $context = shift;

    my $store;
    my $tid = threadid();
    $context->{tid} = $tid;
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    nosqlthreadingdebug('[' . $tid . '] reader thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        $store = &{$context->{get_store}}(); #$reader_connection_name);
        my $blocksize = $context->{blocksize} // $store->get_defaultblockcount();
        nosqlthreadingdebug('[' . $tid . '] reader thread waiting for consumer threads',getlogger(__PACKAGE__));
        while ((_get_other_threads_state($context->{errorstates},$tid) & $RUNNING) == 0) { #wait on cosumers to come up
            #yield();
            sleep($thread_sleep_secs);
        }
        my $i = 0;
        my $cursor = 0;
        my $state = $RUNNING; #start at first
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0) { #as long there is one running consumer and no defunct consumer
            fetching_entries($store,$context->{scan_pattern},$i,$blocksize,getlogger(__PACKAGE__));
            ($cursor, my $rowblock) = $store->scan($cursor,$context->{scan_pattern},$blocksize);
            my $realblocksize = scalar @$rowblock;
            my %packet :shared = ();
            $packet{rows} = $rowblock;
            $packet{size} = $realblocksize;
            $packet{row_offset} = $i;
            $context->{queue}->enqueue(\%packet); #$packet);
            $blockcount++;
            #wait if thequeue is full and there there is one running consumer
            while (((($state = _get_other_threads_state($context->{errorstates},$tid)) & $RUNNING) == $RUNNING) and $context->{queue}->pending() >= $context->{threadqueuelength}) {
                #yield();
                sleep($thread_sleep_secs);
            }
            $i += $realblocksize;
            unless ($cursor) {
                nosqlthreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                last;
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0)) {
            nosqlthreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : 'no running consumer threads') . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ') ...'
            ,getlogger(__PACKAGE__));
        }
    };
    nosqlthreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub _process {

    my $context = shift;

    my $rowblock_result = 1;
    my $tid = threadid();
    $context->{tid} = $tid;
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    nosqlthreadingdebug('[' . $tid . '] processor thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        if (defined $context->{init_process_context_code} and 'CODE' eq ref $context->{init_process_context_code}) {
            &{$context->{init_process_context_code}}($context);
        }
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {

                    processing_entries($tid,$packet->{row_offset},$packet->{size},getlogger(__PACKAGE__));

                    $rowblock_result = &{$context->{process_code}}($context, $packet->{rows},$packet->{row_offset});

                    $blockcount++;

                    if (not $rowblock_result) {
                        nosqlthreadingdebug('[' . $tid . '] shutting down processor thread (processing block NOK) ...',getlogger(__PACKAGE__));
                        last;
                    }

                } else {
                    nosqlthreadingdebug('[' . $tid . '] shutting down processor thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    my $err = $@;
    nosqlthreadingdebug($err ? '[' . $tid . '] processor thread error: ' . $err : '[' . $tid . '] processor thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    eval {
        if (defined $context->{uninit_process_context_code} and 'CODE' eq ref $context->{uninit_process_context_code}) {
            &{$context->{uninit_process_context_code}}($context);
        }
    };
    lock $context->{errorstates};
    if ($err) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = $COMPLETED; #(not $rowblock_result) ? $ERROR : $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub _get_other_threads_state {
    my ($errorstates,$tid) = @_;
    my $result = 0;
    if (!defined $tid) {
        $tid = threadid();
    }
    if (defined $errorstates and ref $errorstates eq 'HASH') {
        lock $errorstates;
        foreach my $threadid (keys %$errorstates) {
            if ($threadid != $tid) {
                $result |= $errorstates->{$threadid};
            }
        }
    }
    return $result;
}

sub _get_stop_consumer_thread {
    my ($context,$tid) = @_;
    my $result = 1;
    my $other_threads_state;
    my $reader_state;
    my $queuesize;
    {
        my $errorstates = $context->{errorstates};
        lock $errorstates;
        $other_threads_state = _get_other_threads_state($errorstates,$tid);
        $reader_state = $errorstates->{$context->{readertid}};
    }
    $queuesize = $context->{queue}->pending();
    if (($other_threads_state & $ERROR) == 0 and ($queuesize > 0 or $reader_state == $RUNNING)) {
        $result = 0;
        #keep the consumer thread running if there is no defunct thread and queue is not empty or reader is still running
    }

    if ($result) {
        nosqlthreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
                            (($other_threads_state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ', ' .
                            ($queuesize > 0 ? 'blocks pending' : 'no blocks pending') . ', ' .
                            ($reader_state == $RUNNING ? 'reader thread running' : 'reader thread not running') . ') ...'
        ,getlogger(__PACKAGE__));
    }

    return $result;

}

sub _create_process_context {

    my $context = {};
    foreach my $ctx (@_) {
        if (defined $ctx and 'HASH' eq ref $ctx) {
            foreach my $key (keys %$ctx) {
                $context->{$key} = $ctx->{$key};
                #delete $ctx->{$key};
            }
        }
    }
    return $context;

}

1;
