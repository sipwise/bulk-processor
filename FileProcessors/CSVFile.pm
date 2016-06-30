package FileProcessors::CSVFile;
use strict;

## no critic

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

my $default_lineseparator = '\\n\\r|\\r|\\n';
my $default_fieldseparator = ",";
my $default_encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $numofthreads = $cpucount; #3;
#my $multithreading = 0;
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
