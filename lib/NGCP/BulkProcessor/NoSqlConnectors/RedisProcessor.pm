package NGCP::BulkProcessor::NoSqlConnectors::RedisProcessor;
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
    get_threadqueuelength
);
use NGCP::BulkProcessor::Logging qw(
    getlogger
    nosqlthreadingdebug
    nosqlprocessingstarted
    nosqlprocessingdone
    fetching_entries
    processing_entries
    enable_threading_info
);

use NGCP::BulkProcessor::LogError qw(
    nosqlprocessingfailed
);

use NGCP::BulkProcessor::Utils qw(threadid);

use NGCP::BulkProcessor::NoSqlConnectors::Redis qw(get_scan_args);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

    process_entries

);

my $nosqlprocessing_threadqueuelength = 10;

#my $reader_connection_name = 'reader';

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;
my $STOP = 8;

sub process_entries {

    my %params = @_;
    my ($get_store,
        $scan_pattern,
        $type,
        $process_code,
        $static_context,
        $blocksize,
        $init_process_context_code,
        $uninit_process_context_code,
        $destroy_reader_stores_code,
        $multithreading,
        $nosqlprocessing_threads) = @params{qw/
            get_store
            scan_pattern
            type
            process_code
            static_context
            blocksize
            init_process_context_code
            uninit_process_context_code
            destroy_reader_stores_code
            multithreading
            nosqlprocessing_threads
        /};

    if (ref $get_store eq 'CODE') {
        
        nosqlprocessingstarted(&$get_store(undef,0),$scan_pattern,getlogger(__PACKAGE__));

        my $errorstate = $RUNNING;
        my $tid = threadid();

        if ($enablemultithreading and $multithreading and $cpucount > 1) {

            $nosqlprocessing_threads //= $cpucount;

            my $reader;
            my %processors = ();
            my %errorstates :shared = ();
            my $queue = Thread::Queue->new();

            nosqlthreadingdebug('shutting down connections ...',getlogger(__PACKAGE__));
            
            #$store->disconnect();
            my $default_connection = &$get_store(undef,0);
            my $default_connection_reconnect = $default_connection->is_connected();
            $default_connection->disconnect();
            
            nosqlthreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            threadqueuelength    => $nosqlprocessing_threadqueuelength,
                                            get_store               => $get_store,
                                            scan_pattern            => $scan_pattern,
                                            type                 => $type,
                                            blocksize            => $blocksize,
                                            destroy_stores_code => $destroy_reader_stores_code,
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

            my $signal_handler = sub {
                my $tid = threadid();
                $errorstate = $STOP;
                enable_threading_info(1);
                nosqlthreadingdebug("[$tid] interrupt signal received",getlogger(__PACKAGE__));
                #print("[$tid] interrupt signal received"); 
                #_info($context,"interrupt signal received");
                #$result = 0;
                my $errorstates = \%errorstates;
                lock $errorstates;
                $errorstates->{$tid} = $STOP;
            };
            local $SIG{TERM} = $signal_handler;
	        local $SIG{INT} = $signal_handler;
	        local $SIG{QUIT} = $signal_handler;
	        local $SIG{HUP} = $signal_handler;

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

            $errorstate = $COMPLETED if $errorstate == $RUNNING;
            $errorstate |= (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);
            
            nosqlthreadingdebug('restoring connections ...',getlogger(__PACKAGE__));
            
            if ($default_connection_reconnect) {
                $default_connection = &$get_store(undef,1);
            }

        } else {

            my $store = &$get_store(undef,1); #$reader_connection_name);
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
                    ($cursor, my $rowblock) = $store->scan($cursor,get_scan_args($scan_pattern,$blocksize,$type));
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
            $store->disconnect();
            
        }

        if ($errorstate == $COMPLETED) {
            nosqlprocessingdone(&$get_store(undef,0),$scan_pattern,getlogger(__PACKAGE__));
            return 1;
        } else {
            nosqlprocessingfailed(&$get_store(undef,0),$scan_pattern,getlogger(__PACKAGE__));
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
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0 and ($state & $STOP) == 0) { #as long there is one running consumer and no defunct consumer
            fetching_entries($store,$context->{scan_pattern},$i,$blocksize,getlogger(__PACKAGE__));
            ($cursor, my $rowblock) = $store->scan_shared($cursor,get_scan_args($context->{scan_pattern},$blocksize,$context->{type}));
            my $realblocksize = scalar @$rowblock;
            my %packet :shared = ();
            $packet{rows} = $rowblock;
            $packet{size} = $realblocksize;
            $packet{row_offset} = $i;
            $context->{queue}->enqueue(\%packet); #$packet);
            $blockcount++;
            #wait if thequeue is full and there there is one running consumer
            while (((($state = _get_other_threads_state($context->{errorstates},$tid)) & $RUNNING) == $RUNNING) and $context->{queue}->pending() >= get_threadqueuelength($context->{threadqueuelength})) {
                #yield();
                sleep($thread_sleep_secs);
            }
            $i += $realblocksize;
            unless ($cursor) {
                nosqlthreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                last;
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0 and ($state & $ERROR) == 0)) {
            nosqlthreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : uc('no running consumer threads')) . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : uc('defunct thread(s)')) . ', ' .
                              (($state & $STOP) == 0 ? 'no thread(s) stopping by signal' : uc('thread(s) stopping by signal')) . ') ...'
            ,getlogger(__PACKAGE__));
        }
    };
    #nosqlthreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    #lock $context->{errorstates};
    #if ($@) {
    #    $context->{errorstates}->{$tid} = $ERROR;
    #} else {
    #    $context->{errorstates}->{$tid} = $COMPLETED;
    #}
    #return $context->{errorstates}->{$tid};
    nosqlthreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    # stop the consumer:
    # $context->{queue}->enqueue(undef);
    if (defined $store) {
        # if thread cleanup has a problem...
        $store->disconnect();
    }
    if (defined $context->{destroy_stores_code} and 'CODE' eq ref $context->{destroy_stores_code}) {
        &{$context->{destroy_stores_code}}();
    }
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } elsif ($context->{errorstates}->{$tid} != $STOP) {
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
    } elsif ($context->{errorstates}->{$tid} != $STOP) {
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
    if (($other_threads_state & $ERROR) == 0 and ($other_threads_state & $STOP) == 0 and ($queuesize > 0 or $reader_state == $RUNNING)) {
        $result = 0;
        #keep the consumer thread running if there is no defunct thread and queue is not empty or reader is still running
    }

    if ($result) {
        nosqlthreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
                            (($other_threads_state & $ERROR) == 0 ? 'no defunct thread(s)' : uc('defunct thread(s)')) . ', ' .
                            (($other_threads_state & $STOP) == 0 ? 'no thread(s) stopping by signal' : uc('thread(s) stopping by signal')) . ', ' .
                            ($queuesize > 0 ? 'blocks pending' : uc('no blocks pending')) . ', ' .
                            ($reader_state == $RUNNING ? 'reader thread running' : uc('reader thread not running')) . ') ...'
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
