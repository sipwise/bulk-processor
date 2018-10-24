package NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure;
use strict;

## no critic

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $base_structure = new NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure(

);

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;
    (
        $self->{name},
    ) = @params{qw/
        name
    /};

    return $self;

}

sub 

1;
