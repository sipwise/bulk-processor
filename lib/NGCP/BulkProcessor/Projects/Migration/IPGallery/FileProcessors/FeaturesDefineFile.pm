package NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile;
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
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser qw(
    create_grammar
    parse
);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $lineseparator = '\\n(?=(?:\d+\\n))';
my $encoding = 'UTF-8';

my $buffersize = 1400; # 512 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 2; #3;
#my $multithreading = 0;
my $blocksize = 1000;  #2000;

my $stoponparseerrors = 1; #1;
my $parselines = 0;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    $self->{numofthreads} = shift // $default_numofthreads;
    $self->{line_separator} = $lineseparator;
    $self->{encoding} = $encoding;
    $self->{buffersize} = $buffersize;
    $self->{threadqueuelength} = $threadqueuelength;
    #$self->{multithreading} = $multithreading;
    $self->{blocksize} = $blocksize;
    $self->{parselines} = $parselines;
    $self->{stoponparseerrors} = $stoponparseerrors;

    bless($self,$class);

    #restdebug($self,__PACKAGE__ . ' file processor created',getlogger(__PACKAGE__));

    return $self;

}

sub init_reader_context {

    my $self = shift;
    my ($context) = @_;

    if ($self->{parselines}) {
        eval {
            $context->{grammar} = create_grammar();
        };
        if ($@) {
            $context->{error_count} = $context->{error_count} + 1;
            fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
        }
    }

}

sub extractfields {
    my ($context,$line_ref) = @_;

    return undef if length($$line_ref) == 0;
    #return undef if $$line_ref =~ /^#/;

    if ($context->{instance}->{parselines}) {
        my $row = undef;
        eval {
            $row = parse($line_ref,$context->{grammar});
        };
        if ($@) {
            if ($context->{instance}->{stoponparseerrors}) {
                $context->{error_count} = $context->{error_count} + 1;
                fileprocessingerror($context->{filename},'record ' . $context->{linesread} . ' - ' . $@,getlogger(__PACKAGE__));
            } else {
                $context->{warning_count} = $context->{warning_count} + 1;
                fileprocessingwarn($context->{filename},'record ' . $context->{linesread} . ' - ' . $@,getlogger(__PACKAGE__));
            }
        }
        return $row;
    } else {
        return $$line_ref;
    }

}

sub stoponparseerrors {
    my $self = shift;
    $self->{stoponparseerrors} = shift if @_;
    return $self->{stoponparseerrors};
}

sub parselines {
    my $self = shift;
    $self->{parselines} = shift if @_;
    return $self->{parselines};
}

1;
