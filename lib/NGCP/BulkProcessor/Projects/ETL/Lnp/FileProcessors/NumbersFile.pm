package NGCP::BulkProcessor::Projects::ETL::Lnp::FileProcessors::NumbersFile;
use strict;

## no critic

use Encode qw(decode);

use NGCP::BulkProcessor::Logging qw(
    getlogger
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingerror
    fileprocessingwarn
);

use NGCP::BulkProcessor::Projects::ETL::Lnp::Settings qw(
    $expand_numbers_code
);

use NGCP::BulkProcessor::FileProcessor;

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $lineseparator = '\\r\\n|\\r|\\n|\\s'; #\\n\\r
my $default_encoding = 'UTF-8';

my $buffersize = 1000 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 3;
#my $multithreading = 0;
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    $self->{numofthreads} = shift // $default_numofthreads;
    $self->{line_separator} = $lineseparator;
    $self->{field_separator} = undef;
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
    return $expand_numbers_code->($context,$$line_ref);

}

1;
