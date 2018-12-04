package NGCP::BulkProcessor::FileProcessors::CSVFileSimple;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::FileProcessor;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $default_lineseparator = '\\r\\n|\\r|\\n'; #\\n\\r
my $default_fieldseparator = ",";
my $default_encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 3;
#my $multithreading = 0;
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    $self->{numofthreads} = shift // $default_numofthreads;
    $self->{line_separator} = shift // $default_lineseparator;
    $self->{field_separator} = shift // $default_fieldseparator;
    $self->{encoding} = shift // $default_encoding;
    $self->{buffersize} = $buffersize;
    $self->{threadqueuelength} = $threadqueuelength;
    #$self->{multithreading} = $multithreading;
    $self->{blocksize} = $blocksize;

    bless($self,$class);

    #restdebug($self,__PACKAGE__ . ' file processor created',getlogger(__PACKAGE__));

    return $self;

}

sub extractfields {
    my ($context,$line_ref) = @_;
    my $separator = $context->{instance}->{field_separator};
    my @fields = split(/$separator/,$$line_ref,-1);
    return \@fields;
}

1;
