package FileProcessor;
use strict;

## no critic

use threads qw(yield);
use threads::shared;
use Thread::Queue;

use Time::HiRes qw(sleep);

use Globals qw(
    $enablemultithreading
    $cpucount
);
use Logging qw(
    getlogger
    filethreadingdebug
);


#    fetching_rows
#    writing_rows
#    processing_rows

use LogError qw(
    fileerror
    notimplementederror
);

use Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw();

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;

sub new {

    my $class = shift;
    my $self = bless {}, $class;

    $self->{encoding} = undef;
    $self->{buffersize} = undef;
    $self->{threadqueuelength} = undef;
    $self->{numofthreads} = undef;
    $self->{multithreading} = undef;
    $self->{blocksize} = undef;

    return $self;

}

sub init_reader_context {

    my $self = shift;
    my ($context) = @_;
    # init stuff available to the reader loop
    # invoked after thread was forked, as
    # required by e.g. Marpa R2

}

sub process {

    my $self = shift;

    my ($file,$process_code) = @_;

    if (ref $process_code eq 'CODE') {

        #if ($linecount > 0) {XXXX
        #    tableprocessingstarted($db,$tablename,$rowcount,getlogger($class ));
        #} else {
        #    processzerorowcount($db,$tablename,$rowcount,getlogger(__PACKAGE__));
        #    return;
        #}

        my $errorstate = $RUNNING;
        my $tid = threadid();

        if ($enablemultithreading and $self->{multithreading} and $cpucount > 1) { # and $multithreaded) { # definitely no multithreading when CSVDB is involved

            my $reader;
            my %processors = ();
            my %errorstates :shared = ();
            my $queue = Thread::Queue->new();

            filethreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            instance             => $self,
                                            filename             => $file,
                                          });

            for (my $i = 0; $i < $self->{numofthreads}; $i++) {
                filethreadingdebug('starting processor thread ' . ($i + 1) . ' of ' . $self->{numofthreads},getlogger(__PACKAGE__));
                my $processor = threads->create(\&_process,
                                              { queue                => $queue,
                                                errorstates          => \%errorstates,
                                                readertid              => $reader->tid(),
                                                process_code         => $process_code,
                                                instance             => $self,
                                              });
                if (!defined $processor) {
                    filethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $self->{numofthreads} . ' NOT started',getlogger(__PACKAGE__));
                }
                $processors{$processor->tid()} = $processor;
            }

            $reader->join();
            filethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            while ((scalar keys %processors) > 0) {
                foreach my $processor (values %processors) {
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        filethreadingdebug('processor thread tid ' . $processor->tid() . ' joined',getlogger(__PACKAGE__));
                    }
                }
                sleep($thread_sleep_secs);
            }

            $errorstate = (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);

        } else {

            my $context = { instance => $self, };
            my $rowblock_result = 1;
            eval {

                my $init_reader_context_code = $self->can('init_reader_context');
                if (defined $init_reader_context_code) {
                    &$init_reader_context_code($self,$context);
                }
                my $extractlines_code = (ref $self)->can('extractlines');
                if (!defined $extractlines_code) {
                    notimplementederror((ref $self) . ': ' . 'extractlines class method not implemented',getlogger(__PACKAGE__));
                }
                my $extractfields_code = (ref $self)->can('extractfields');
                if (!defined $extractfields_code) {
                    notimplementederror((ref $self) . ': ' . 'extractfields class method not implemented',getlogger(__PACKAGE__));
                }

                local *INPUTFILE;
                if (not open (INPUTFILE, '<:encoding(' . $self->{encoding} . ')', $file)) {
                    fileerror('processing file - cannot open file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
                    return;
                }
                binmode INPUTFILE;

                my $buffer = undef;
                my $chunk = undef;
                my $n = 0;
                my $charsread = 0;

                my $i = 0;
                while (1) {
                    my @lines = ();
                    while ((scalar @lines) < $self->{blocksize} and defined ($n = read(INPUTFILE,$chunk,$self->{buffersize})) and $n != 0) {
                        if (defined $buffer) {
                            $buffer .= $chunk;
                        } else {
                            $buffer = $chunk;
                        }
                        $charsread += $n
                        last unless &$extractlines_code($context,\$buffer,\@lines);
                    }

                    if (not defined $n) {
                        fileerror('processing file - error reading file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
                        close(INPUTFILE);
                        last;
                    } else {
                        if ($n == 0 && defined $buffer) {
                            push(@lines,$buffer);
                        }
                        my @rowblock = ();
                        foreach my $line (@lines) {
                            my %row = ();
                            if (&$extractfields_code($context,\$line,\%row)) {
                                push(@rowblock,\%row);
                            }
                        }
                        my $realblocksize = scalar @rowblock;
                        if ($realblocksize > 0) {
                            #processing_rows($tid,$i,$realblocksize,$rowcount,getlogger(__PACKAGE__));

                            $rowblock_result = &$process_code(\@rowblock,$i);

                            $i += $realblocksize;
                            if ($n == 0 || not $rowblock_result) {
                                last;
                            }
                        } else {
                            last;
                        }
                    }
                }
                close(INPUTFILE);

            };

            if ($@) {
                $errorstate = $ERROR;
            } else {
                $errorstate = (not $rowblock_result) ? $ERROR : $COMPLETED;
            }

        }

        if ($errorstate == $COMPLETED) {
            #tableprocessingdone($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            return 1;
        } else {
            #tableprocessingfailed($db,$tablename,$rowcount,getlogger(__PACKAGE__));
        }

    }

    return 0;

}

sub _reader {

    my $context = shift;

    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    filethreadingdebug('[' . $tid . '] reader thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {

        my $init_reader_context_code = $context->{instance}->can('init_reader_context');
        if (defined $init_reader_context_code) {
            &$init_reader_context_code($context->{instance},$context);
        }
        my $extractlines_code = (ref $context->{instance})->can('extractlines');
        if (!defined $extractlines_code) {
            notimplementederror((ref $context->{instance}) . ': ' . 'extractlines class method not implemented',getlogger(__PACKAGE__));
        }
        my $extractfields_code = (ref $context->{instance})->can('extractfields');
        if (!defined $extractfields_code) {
            notimplementederror((ref $context->{instance}) . ': ' . 'extractfields class method not implemented',getlogger(__PACKAGE__));
        }

        local *INPUTFILE_READER;
        if (not open (INPUTFILE_READER, '<:encoding(' . $context->{instance}->{encoding} . ')', $context->{filename})) {
            fileerror('processing file - cannot open file ' . $context->{filename} . ': ' . $!,getlogger(__PACKAGE__));
            return;
        }
        binmode INPUTFILE_READER;

        filethreadingdebug('[' . $tid . '] reader thread waiting for consumer threads',getlogger(__PACKAGE__));
        while ((_get_other_threads_state($context->{errorstates},$tid) & $RUNNING) == 0) { #wait on cosumers to come up
            #yield();
            sleep($thread_sleep_secs);
        }

        my $buffer = undef;
        my $chunk = undef;
        my $n = 0;
        my $charsread = 0;

        my $i = 0;
        my $state = $RUNNING; #start at first
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0) { #as long there is one running consumer and no defunct consumer
            my @lines = ();
            while ((scalar @lines) < $context->{instance}->{blocksize} and defined ($n = read(INPUTFILE_READER,$chunk,$context->{instance}->{buffersize})) and $n != 0) {
                if (defined $buffer) {
                    $buffer .= $chunk;
                } else {
                    $buffer = $chunk;
                }
                $charsread += $n;
                last unless &$extractlines_code($context,\$buffer,\@lines);
                yield();
            }
            if (not defined $n) {
                fileerror('processing file - error reading file ' . $context->{filename} . ': ' . $!,getlogger(__PACKAGE__));
                close(INPUTFILE_READER);
                last;
            } else {
                if ($n == 0 && defined $buffer) {
                    push(@lines,$buffer);
                }
                my @rowblock :shared = ();
                foreach my $line (@lines) {
                    my %row :shared = ();
                    if (&$extractfields_code($context,\$line,\%row)) {
                        push(@rowblock,\%row);
                        yield();
                    }
                }
                my $realblocksize = scalar @rowblock;
                my %packet :shared = ();
                $packet{rows} = \@rowblock;
                $packet{size} = $realblocksize;
                $packet{row_offset} = $i;
                if ($realblocksize > 0) {
                    $context->{queue}->enqueue(\%packet); #$packet);
                    $blockcount++;
                    #wait if thequeue is full and there there is one running consumer
                    while (((($state = _get_other_threads_state($context->{errorstates},$tid)) & $RUNNING) == $RUNNING) and $context->{queue}->pending() >= $context->{instance}->{threadqueuelength}) {
                        #yield();
                        sleep($thread_sleep_secs);
                    }
                    $i += $realblocksize;
                    if ($n == 0) {
                        filethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                        last;
                    }
                } else {
                    $context->{queue}->enqueue(\%packet); #$packet);
                    filethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0)) {
            filethreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : 'no running consumer threads') . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ') ...'
            ,getlogger(__PACKAGE__));
        }
        close(INPUTFILE_READER);
    };

    filethreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
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
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    filethreadingdebug('[' . $tid . '] processor thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {

                    #processing_rows($tid,$packet->{row_offset},$packet->{size},$context->{rowcount},getlogger(__PACKAGE__));

                    $rowblock_result = &{$context->{process_code}}($packet->{rows},$packet->{row_offset});

                    $blockcount++;

                    if (not $rowblock_result) {
                        filethreadingdebug('[' . $tid . '] shutting down processor thread (processing block NOK) ...',getlogger(__PACKAGE__));
                        last;
                    }

                } else {
                    filethreadingdebug('[' . $tid . '] shutting down processor thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    filethreadingdebug($@ ? '[' . $tid . '] processor thread error: ' . $@ : '[' . $tid . '] processor thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
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
        filethreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
                            (($other_threads_state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ', ' .
                            ($queuesize > 0 ? 'blocks pending' : 'no blocks pending') . ', ' .
                            ($reader_state == $RUNNING ? 'reader thread running' : 'reader thread not running') . ') ...'
        ,getlogger(__PACKAGE__));
    }

    return $result;

}

1;



