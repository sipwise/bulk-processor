package Projects::Migration::IPGallery::FileProcessors::SubscriberDefineFile;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

#use Globals qw(
#    $cpucount
#);

use Logging qw(
    getlogger
);
use LogError qw(
    fileprocessingerror
    fileprocessingwarn
);

use FileProcessor;

require Exporter;
our @ISA = qw(Exporter FileProcessor);
our @EXPORT_OK = qw();

my $lineseparator = '\\n';
my $fieldseparator = " +";
my $encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 3;
#my $multithreading = 0;
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = FileProcessor->new(@_);

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
    my @fields = split(/$separator/,$$line_ref,-1);
    return \@fields;
}

1;
