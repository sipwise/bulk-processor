package Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

use Marpa::R2;
use Data::Dumper;

use Globals qw(
    $cpucount
);

use Logging qw(
    getlogger
);
use LogError qw(
    fileprocessingerror
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
my $multithreading = 0;
my $blocksize = 2000;

sub new {

    my $class = shift;

    my $self = FileProcessor->new(@_);

    $self->{line_separator} = $lineseparator;
    $self->{encoding} = $encoding;
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

    eval {
        $context->{grammar} = create_grammar();
    };
    if ($@) {
        fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
    }

}

sub extractfields {
    my ($context,$line_ref) = @_;
    return undef if length($$line_ref) == 0;
    my $row = undef;
    eval {
        $row = parse($line_ref,$context->{grammar});
    };
    if ($@) {
        fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
    }
    return $row;
}

1;
