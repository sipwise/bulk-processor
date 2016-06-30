package FileProcessors::CSVFile;
use strict;

## no critic

use threads;
use threads::shared qw(share);

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Globals qw(
    $cpucount
);

use Logging qw(
    getlogger
);

use FileProcessor;

require Exporter;
our @ISA = qw(Exporter FileProcessor);
our @EXPORT_OK = qw();

my $default_lineseparator = "\n";
my $default_fieldseparator = ",";
my $default_encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $numofthreads = $cpucount; #3;
my $multithreading = 1;
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = FileProcessor->new(@_);

    $self->{line_separator} = shift // $default_lineseparator;
    $self->{field_separator} = shift // $default_fieldseparator;
    $self->{encoding} = shift // $default_encoding;
    $self->{buffersize} = $buffersize;
    $self->{threadqueuelength} = $threadqueuelength;
    $self->{numofthreads} = $numofthreads;
    $self->{multithreading} = $multithreading;
    $self->{blocksize} = $blocksize;

    bless($self,$class);

    #restdebug($self,__PACKAGE__ . ' file processor created',getlogger(__PACKAGE__));

    return $self;

}

sub init_reader_context {

    my $self = shift;
    my ($context) = @_;
    # init stuff available to the reader loop
    # invoked after thread was forked, as
    # required by e.g. Marpa R2

}

sub extractlines {
    my ($context,$buffer_ref,$lines) = @_;
    my $separator = $context->{instance}->{line_separator};
    my $last_line;
    foreach my $line (split(/$separator/,$$buffer_ref,-1)) {
        $last_line = $line;
        push(@$lines,$line);
    }
    #$count--;
    $$buffer_ref = $last_line;
    pop @$lines;

    return 1;
}

sub extractfields {
    my ($context,$line_ref,$row) = @_;
    $row->{test} = share($$line_ref);

    return 1;
}

1;
