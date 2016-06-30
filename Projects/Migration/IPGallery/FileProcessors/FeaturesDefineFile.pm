package Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use Globals qw(
    $cpucount
);

use Logging qw(
    getlogger
);
use LogError qw(
    fileprocessingerror
    fileprocessingwarn
);

use FileProcessor;
use Projects::Migration::IPGallery::FeaturesDefineParser qw(
    create_grammar
    parse
);

require Exporter;
our @ISA = qw(Exporter FileProcessor);
our @EXPORT_OK = qw();

my $lineseparator = '\\n(?=(?:\d+\\n))';
my $encoding = 'UTF-8';

my $buffersize = 1400; # 512 * 1024;
my $threadqueuelength = 10;
my $numofthreads = $cpucount; #3;
#my $multithreading = 0;
my $blocksize = 2000;

my $stoponparseerrors = 0; #1;
my $parselines = 0;

sub new {

    my $class = shift;

    my $self = FileProcessor->new(@_);

    $self->{line_separator} = $lineseparator;
    $self->{encoding} = $encoding;
    $self->{buffersize} = $buffersize;
    $self->{threadqueuelength} = $threadqueuelength;
    $self->{numofthreads} = $numofthreads;
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
            fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
        }
    }

}

sub extractfields {
    my ($context,$line_ref) = @_;

    return undef if length($$line_ref) == 0;

    if ($context->{instance}->{parselines}) {
        my $row = undef;
        eval {
            $row = parse($line_ref,$context->{grammar});
        };
        if ($@) {
            if ($context->{instance}->{stoponparseerrors}) {
                fileprocessingerror($context->{filename},'record ' . $context->{linesread} . ' - ' . $@,getlogger(__PACKAGE__));
            } else {
                fileprocessingwarn($context->{filename},'record ' . $context->{linesread} . ' - ' . $@,getlogger(__PACKAGE__));
            }
        }
        return $row;
    } else {
        return $$line_ref;
    }

}

1;
