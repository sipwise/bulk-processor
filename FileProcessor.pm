package FileProcessor;
use strict;

## no critic

use threads qw(yield);
use threads::shared;
use Thread::Queue;

use Time::HiRes qw(sleep);

use Logging qw(
    getlogger

    fetching_rows
    writing_rows
    processing_rows

    filethreadingdebug
);

use LogError qw(

);

use Table qw(get_rowhash);
use Array qw(setcontains contains);
use Utils qw(round threadid);

use Globals qw(
$enablemultithreading
$cpucount
$cells_transfer_memory_limit);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    process_file
);

my $tabletransfer_threadqueuelength = 5; #100; #30; #5; # ... >= 1
my $minblocksize = 100;
my $maxblocksize = 100000;
my $minnumberofchunks = 10;

my $tableprocessing_threadqueuelength = 10;
my $tableprocessing_threads = $cpucount; #3;

my $reader_connection_name = 'reader';
#my $writer_connection_name = 'writer';

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;

my $default_lineseparator = "\n";
my $default_encoding = 'UTF-8';


sub process_file {

    my ($reader_code,$process_code,$multithreading,$blocksize) = @_;

    if (ref $reader_code eq 'CODE') {

        $lineseparator //= $default_lineseparator = "\n";
    $encoding //= $default_encoding;

    _get_linecount


        if ($linecount > 0) {
            tableprocessingstarted($db,$tablename,$rowcount,getlogger(__PACKAGE__));
        } else {
            processzerorowcount($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            return;
        }







        my $errorstate = $RUNNING;

        my $tid = threadid();

        my $blocksize;

        if ($enablemultithreading and $multithreading and $cpucount > 1) { # and $multithreaded) { # definitely no multithreading when CSVDB is involved

            $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,1,$tableprocessing_threadqueuelength);

            my $reader;
            #my $processor;
            my %processors = ();

            my %errorstates :shared = ();
            #$errorstates{$tid} = $errorstate;

            #my $readererrorstate :shared = 1;
            #my $processorerrorstate :shared = 1;

            my $queue = Thread::Queue->new();

            tablethreadingdebug('shutting down db connections ...',getlogger(__PACKAGE__));

            $db->db_disconnect();
            #undef $db;
            my $default_connection = &$get_db(undef,0);
            my $default_connection_reconnect = $default_connection->is_connected();
            $default_connection->db_disconnect();

            tablethreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            #readererrorstate_ref => \$readererrorstate,
                                            #writererrorstate_ref => \$processorerrorstate,
                                            threadqueuelength    => $tableprocessing_threadqueuelength,
                                            get_db               => $get_db,
                                            tablename            => $tablename,
                                            selectstatement      => $selectstatement,
                                            blocksize            => $blocksize,
                                            rowcount             => $rowcount,
                                            #logger               => $logger,
                                            values_ref           => \@values,
                                          });

            for (my $i = 0; $i < $tableprocessing_threads; $i++) {
                tablethreadingdebug('starting processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads,getlogger(__PACKAGE__));
                my $processor = threads->create(\&_process,
                                              { queue                => $queue,
                                                errorstates          => \%errorstates,
                                                readertid              => $reader->tid(),
                                                #readererrorstate_ref => \$readererrorstate,
                                                #processorerrorstate_ref => \$processorerrorstate,
                                                process_code         => $process_code,
                                                blocksize            => $blocksize,
                                                rowcount             => $rowcount,
                                                #logger               => $logger,
                                              });
                if (!defined $processor) {
                    tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT started',getlogger(__PACKAGE__));
                }
                $processors{$processor->tid()} = $processor;
                #push (@processors,$processor);
            }

            #$reader->join();
            #tablethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            #for (my $i = 0; $i < $tableprocessing_threads; $i++) {
            #    my $processor = $processors[$i];
            #    if (defined $processor) {
            #        $processor->join();
            #        tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' joinded',getlogger(__PACKAGE__));
            #    } else {
            #        tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT joinded',getlogger(__PACKAGE__));
            #    }
            #}

            $reader->join();
            tablethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            #print 'threads running: ' . (scalar threads->list(threads::running));
            #while ((scalar threads->list(threads::running)) > 1 or (scalar threads->list(threads::joinable)) > 0) {
            while ((scalar keys %processors) > 0) {
                #for (my $i = 0; $i < $tableprocessing_threads; $i++) {
                foreach my $processor (values %processors) {
                    #my $processor = $processors[$i];
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        #tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' joinded',getlogger(__PACKAGE__));
                        tablethreadingdebug('processor thread tid ' . $processor->tid() . ' joined',getlogger(__PACKAGE__));
                    }
                    #} else {
                    #    tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT joinded',getlogger(__PACKAGE__));
                    #}
                }
                sleep($thread_sleep_secs);
            }

            #$errorstate = $readererrorstate | $processorerrorstate;
            $errorstate = (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);

            tablethreadingdebug('restoring db connections ...',getlogger(__PACKAGE__));

            #$db = &$get_db($reader_connection_name,1);
            if ($default_connection_reconnect) {
                $default_connection = &$get_db(undef,1);
            }

        } else {

            $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,0,undef);
            #$db->db_disconnect();
            #undef $db;
            #$db = &$get_db($reader_connection_name);

            my $rowblock_result = 1;
            eval {
                $db->db_get_begin($selectstatement,$tablename,@values);

                my $i = 0;
                while (1) {
                    fetching_rows($db,$tablename,$i,$blocksize,$rowcount,getlogger(__PACKAGE__));
                    my $rowblock = $db->db_get_rowblock($blocksize);
                    my $realblocksize = scalar @$rowblock;
                    if ($realblocksize > 0) {
                        processing_rows($tid,$i,$realblocksize,$rowcount,getlogger(__PACKAGE__));

                        $rowblock_result = &$process_code($rowblock,$i);

                        #$target_db->db_do_begin($insertstatement,$targettablename);
                        #$target_db->db_do_rowblock($rowblock);
                        #$target_db->db_finish();
                        $i += $realblocksize;

                        if ($realblocksize < $blocksize || not $rowblock_result) {
                             last;
                        }
                    } else {
                        last;
                    }
                }
                $db->db_finish();

            };

            if ($@) {
                $errorstate = $ERROR;
            } else {
                $errorstate = (not $rowblock_result) ? $ERROR : $COMPLETED;
            }

            $db->db_disconnect();
            #undef $db;

        }

        #$db = &$get_db($controller_name,1);

        if ($errorstate == $COMPLETED) {
            tableprocessingdone($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
            return 1;
        } else {
            tableprocessingfailed($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
        }

    }

    return 0;

}


sub _reader {

    #my ($queue,$readererrorstate_ref,$writererrorstate_ref,$get_db,$tablename,$selectstatement,$blocksize,$rowcount,$logger,@values) = @_;
    my $context = shift;

    my $reader_db;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    tablethreadingdebug('[' . $tid . '] reader thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        local $/ = $context->{lineseparator};

        local *INPUTFILE_READER;
        if (not open (INPUTFILE_READER, '<:encoding(' . $context->{encoding} . ')' . $file)) {
          fileerror('parsing simple format - cannot open file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
          return $config;
        }

        while (my $row = <INPUTFILE_READER>) {
            chomp $row;
            print "$row\n";
        }
        close(INPUTFILE_READER);

        $reader_db = &{$context->{get_db}}(); #$reader_connection_name);
        $reader_db->db_get_begin($context->{selectstatement},$context->{tablename},@{$context->{values_ref}});
        my $i = 0;
        tablethreadingdebug('[' . $tid . '] reader thread waiting for consumer threads',getlogger(__PACKAGE__));
        while ((_get_other_threads_state($context->{errorstates},$tid) & $RUNNING) == 0) { #wait on cosumers to come up
            #yield();
            sleep($thread_sleep_secs);
        }
        my $state = $RUNNING; #start at first
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0) { #as long there is one running consumer and no defunct consumer
            fetching_rows($reader_db,$context->{tablename},$i,$context->{blocksize},$context->{rowcount},getlogger(__PACKAGE__));
            my $rowblock = $reader_db->db_get_rowblock($context->{blocksize});
            my $realblocksize = scalar @$rowblock;
            my $packet = {rows     => $rowblock,
                          size     => $realblocksize,
                          #block    => $i,
                          row_offset => $i};
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
                if ($realblocksize < $context->{blocksize}) {
                    tablethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                $context->{queue}->enqueue(\%packet); #$packet);
                tablethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data - empty block) ...',getlogger(__PACKAGE__));
                last;
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0)) {
            tablethreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : 'no running consumer threads') . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ') ...'
            ,getlogger(__PACKAGE__));
        }
        $reader_db->db_finish();
    };
    # stop the consumer:
    # $context->{queue}->enqueue(undef);
    if (defined $reader_db) {
        # if thread cleanup has a problem...
        $reader_db->db_disconnect();
    }
    tablethreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub _get_linecount {

    my ($file,$lineseparator,$encoding) = @_;

    local $/ = $lineseparator;
    local *INPUTFILE_LINECOUNT;
    if (not open (INPUTFILE_LINECOUNT, '<:encoding(' . $encoding . ')' . $file)) {
        fileerror('get line count - cannot open file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
        return undef;
    }

    my $linecount = 0;
    $linecount++ while <INPUTFILE_LINECOUNT>;
    close(INPUTFILE_LINECOUNT);

    return $linecount;

}

sub _process {

    my $context = shift;

    #my $writer_db;
    my $rowblock_result = 1;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    tablethreadingdebug('[' . $tid . '] processor thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        #$writer_db = &{$context->{get_target_db}}($writer_connection_name);
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {

                    #writing_rows($writer_db,$context->{targettablename},$i,$realblocksize,$context->{rowcount},getlogger(__PACKAGE__));

                    #$writer_db->db_do_begin($context->{insertstatement},$context->{targettablename});
                    #$writer_db->db_do_rowblock($rowblock);
                    #$writer_db->db_finish();

                    #$i += $realblocksize;

                    processing_rows($tid,$packet->{row_offset},$packet->{size},$context->{rowcount},getlogger(__PACKAGE__));

                    $rowblock_result = &{$context->{process_code}}($packet->{rows},$packet->{row_offset});

                    $blockcount++;

                    #$i += $realblocksize;

                    if (not $rowblock_result) {
                        tablethreadingdebug('[' . $tid . '] shutting down processor thread (processing block NOK) ...',getlogger(__PACKAGE__));
                        last;
                    }

                } else {
                    tablethreadingdebug('[' . $tid . '] shutting down processor thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    #if (defined $writer_db) {
    #    $writer_db->db_disconnect();
    #}
    tablethreadingdebug($@ ? '[' . $tid . '] processor thread error: ' . $@ : '[' . $tid . '] processor thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = (not $rowblock_result) ? $ERROR : $COMPLETED;
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
        tablethreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
                            (($other_threads_state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ', ' .
                            ($queuesize > 0 ? 'blocks pending' : 'no blocks pending') . ', ' .
                            ($reader_state == $RUNNING ? 'reader thread running' : 'reader thread not running') . ') ...'
        ,getlogger(__PACKAGE__));
    }

    return $result;

}

1;



