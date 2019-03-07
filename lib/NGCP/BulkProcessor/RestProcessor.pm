package NGCP::BulkProcessor::RestProcessor;
use strict;

## no critic

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
    restthreadingdebug
    restprocessingstarted
    restprocessingdone
    fetching_items
    processing_items
);

use NGCP::BulkProcessor::LogError qw(
    restprocessingfailed
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    init_item
    copy_row
    process_collection
    get_query_string
    override_fields
);

my $collectionprocessing_threadqueuelength = 10;

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;

sub get_query_string {
    my ($filters) = @_;
    my $query = '';
    foreach my $param (keys %$filters) {
        if (length($query) == 0) {
            $query .= '?';
        } else {
            $query .= '&';
        }
        $query .= URI::Escape::uri_escape($param) . '=' . URI::Escape::uri_escape_utf8($filters->{$param});
    }
    return $query;
};

sub override_fields {
    my ($item,$load_recursive) = @_;
    foreach my $override (keys %{$load_recursive->{_overrides}}) {
        $item->{$override} = $load_recursive->{_overrides}->{$override};
    }
}

sub init_item {

    my ($item,$fieldnames) = @_;

    if (defined $fieldnames) {
        # if there are fieldnames defined, we make a member variable for each and set it to undef
        foreach my $fieldname (@$fieldnames) {
            $item->{$fieldname} = undef;
        }
    }

    return $item;

}

sub copy_row {
    my ($item,$row,$fieldnames) = @_;
    if (defined $item and defined $row) {
        my $i;
        if (ref $row eq 'ARRAY') {
            $i = 0;
        } elsif (ref $row eq 'HASH') {
            $i = -1;
        } elsif (ref $row eq ref $item) {
            $i = -2;
        } else {
            $i = -3;
        }
        foreach my $fieldname (@$fieldnames) {
            if ($i >= 0) {
                $item->{$fieldname} = $row->[$i];
                $i++;
            } elsif ($i == -1 or $i == -2) {
                if (exists $row->{$fieldname}) {
                    $item->{$fieldname} = $row->{$fieldname};
                } elsif (exists $row->{uc($fieldname)}) {
                    $item->{$fieldname} = $row->{uc($fieldname)};
                } else {
                    $item->{$fieldname} = undef;
                }
            } else {
                $item->{$fieldname} = $row; #scalar
                last;
            }
        }
    }
    return $item;
}

sub process_collection {

    my %params = @_;
    my ($get_restapi,
        $path_query,
        $post_data,
        $headers,
        $extract_collection_items_params,
        $process_code,
        $static_context,
        $blocksize,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $collectionprocessing_threads) = @params{qw/
            get_restapi
            path_query
            post_data
            headers
            extract_collection_items_params
            process_code
            static_context
            blocksize
            init_process_context_code
            uninit_process_context_code
            multithreading
            collectionprocessing_threads
        /};

    if (ref $get_restapi eq 'CODE') {

        restprocessingstarted(&$get_restapi(),$path_query,getlogger(__PACKAGE__));

        my $errorstate = $RUNNING;
        my $tid = threadid();

        if ($enablemultithreading and $multithreading and $cpucount > 1) {

            $collectionprocessing_threads //= $cpucount;

            my $reader;
            my %processors = ();
            my %errorstates :shared = ();
            my $queue = Thread::Queue->new();

            restthreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            threadqueuelength    => $collectionprocessing_threadqueuelength,
                                            get_restapi               => $get_restapi,
                                            path_query            => $path_query,
                                            headers            => $headers,
                                            blocksize            => $blocksize,
                                            extract_collection_items_params => $extract_collection_items_params,
                                            post_data          => $post_data,
                                          });

            for (my $i = 0; $i < $collectionprocessing_threads; $i++) {
                restthreadingdebug('starting processor thread ' . ($i + 1) . ' of ' . $collectionprocessing_threads,getlogger(__PACKAGE__));
                my $processor = threads->create(\&_process,
                                              _create_process_context($static_context,
                                              { queue                => $queue,
                                                errorstates          => \%errorstates,
                                                readertid              => $reader->tid(),
                                                #path_query            => $path_query,
                                                process_code         => $process_code,
                                                init_process_context_code => $init_process_context_code,
                                                uninit_process_context_code => $uninit_process_context_code,
                                                #blocksize            => $blocksize,
                                              }));
                if (!defined $processor) {
                    restthreadingdebug('processor thread ' . ($i + 1) . ' of ' . $collectionprocessing_threads . ' NOT started',getlogger(__PACKAGE__));
                }
                $processors{$processor->tid()} = $processor;
            }

            $reader->join();
            restthreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            while ((scalar keys %processors) > 0) {
                foreach my $processor (values %processors) {
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        restthreadingdebug('processor thread tid ' . $processor->tid() . ' joined',getlogger(__PACKAGE__));
                    }
                }
                sleep($thread_sleep_secs);
            }

            $errorstate = (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);

        } else {

            my $restapi = &$get_restapi(); #$reader_connection_name);
            $blocksize //= $restapi->get_defaultcollectionpagesize();

            my $context = _create_process_context($static_context,{ tid => $tid });
            my $rowblock_result = 1;
            my $blockcount = 0;
            eval {
                if (defined $init_process_context_code and 'CODE' eq ref $init_process_context_code) {
                    &$init_process_context_code($context);
                }

                my $i = 0;
                while (1) {
                    fetching_items($restapi,$path_query,$i,$blocksize,getlogger(__PACKAGE__));
                    my $collection_page;
                    $collection_page = $restapi->get($restapi->get_collection_page_query_uri($path_query,$blocksize,$blockcount + $restapi->get_firstcollectionpagenum),$headers) unless $post_data;
                    $collection_page = $restapi->post($restapi->get_collection_page_query_uri($path_query,$blocksize,$blockcount + $restapi->get_firstcollectionpagenum),$post_data,$headers) if $post_data;
                    my $rowblock = $restapi->extract_collection_items($collection_page,$blocksize,$blockcount,$extract_collection_items_params);
                    my $realblocksize = scalar @$rowblock;
                    if ($realblocksize > 0) {
                        processing_items($tid,$i,$realblocksize,getlogger(__PACKAGE__));

                        $rowblock_result = &$process_code($context,$rowblock,$i);

                        $i += $realblocksize;
                        $blockcount++;

                        if ($realblocksize < $blocksize || not $rowblock_result) {
                             last;
                        }
                    } else {
                        last;
                    }
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
            restprocessingdone(&$get_restapi(),$path_query,getlogger(__PACKAGE__));
            return 1;
        } else {
            restprocessingfailed(&$get_restapi(),$path_query,getlogger(__PACKAGE__));
        }

    }

    return 0;

}


sub _reader {

    my $context = shift;

    my $restapi;
    my $tid = threadid();
    $context->{tid} = $tid;
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    restthreadingdebug('[' . $tid . '] reader thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        $restapi = &{$context->{get_restapi}}(); #$reader_connection_name);
        my $blocksize = $context->{blocksize} // $restapi->get_defaultcollectionpagesize();
        restthreadingdebug('[' . $tid . '] reader thread waiting for consumer threads',getlogger(__PACKAGE__));
        while ((_get_other_threads_state($context->{errorstates},$tid) & $RUNNING) == 0) { #wait on cosumers to come up
            #yield();
            sleep($thread_sleep_secs);
        }
        my $i = 0;
        my $state = $RUNNING; #start at first
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0) { #as long there is one running consumer and no defunct consumer
            fetching_items($restapi,$context->{path_query},$i,$blocksize,getlogger(__PACKAGE__));

            my $collection_page;
            $collection_page = $restapi->get($restapi->get_collection_page_query_uri($context->{path_query},$blocksize,$blockcount + $restapi->get_firstcollectionpagenum),$context->{headers}) unless $context->{post_data};
            $collection_page = $restapi->post($restapi->get_collection_page_query_uri($context->{path_query},$blocksize,$blockcount + $restapi->get_firstcollectionpagenum),$context->{post_data},$context->{headers}) if $context->{post_data};
            my $rowblock = $restapi->extract_collection_items($collection_page,$blocksize,$blockcount,$context->{extract_collection_items_params});
            my $realblocksize = scalar @$rowblock;
            my %packet :shared = ();
            $packet{rows} = $rowblock;
            $packet{size} = $realblocksize;
            $packet{row_offset} = $i;
            if ($realblocksize > 0) {
                $context->{queue}->enqueue(\%packet); #$packet);
                $blockcount++;
                #wait if thequeue is full and there there is one running consumer
                while (((($state = _get_other_threads_state($context->{errorstates},$tid)) & $RUNNING) == $RUNNING) and $context->{queue}->pending() >= $context->{threadqueuelength}) {
                    #yield();
                    sleep($thread_sleep_secs);
                }
                $i += $realblocksize;
                if ($realblocksize < $blocksize) {
                    restthreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                $context->{queue}->enqueue(\%packet); #$packet);
                restthreadingdebug('[' . $tid . '] reader thread is shutting down (end of data - empty block) ...',getlogger(__PACKAGE__));
                last;
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0)) {
            restthreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : 'no running consumer threads') . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ') ...'
            ,getlogger(__PACKAGE__));
        }
    };
    restthreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
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

    restthreadingdebug('[' . $tid . '] processor thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        if (defined $context->{init_process_context_code} and 'CODE' eq ref $context->{init_process_context_code}) {
            &{$context->{init_process_context_code}}($context);
        }
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {

                    processing_items($tid,$packet->{row_offset},$packet->{size},getlogger(__PACKAGE__));

                    $rowblock_result = &{$context->{process_code}}($context, $packet->{rows},$packet->{row_offset});

                    $blockcount++;

                    if (not $rowblock_result) {
                        restthreadingdebug('[' . $tid . '] shutting down processor thread (processing block NOK) ...',getlogger(__PACKAGE__));
                        last;
                    }

                } else {
                    restthreadingdebug('[' . $tid . '] shutting down processor thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    my $err = $@;
    restthreadingdebug($err ? '[' . $tid . '] processor thread error: ' . $err : '[' . $tid . '] processor thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
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
        restthreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
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
