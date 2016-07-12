package NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::BatchFile;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingerror
    fileprocessingwarn
);

use NGCP::BulkProcessor::FileProcessor;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $lineseparator = '\\n';
my $fieldseparator = " +";
my $encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 3;
#my $multithreading = 0;
my $blocksize = 500;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    $self->{numofthreads} = shift // $default_numofthreads;
    $self->{line_separator} = $lineseparator;
    $self->{field_separator} = $fieldseparator;
    $self->{encoding} = $encoding;
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
    $$line_ref =~ s/^ +//;
    $$line_ref =~ s/ +$//;
    return undef if length($$line_ref) == 0;
    return undef if $$line_ref =~ /^#/;
    my @fields = split(/$separator/,$$line_ref,-1);
    return \@fields;
}

1;
