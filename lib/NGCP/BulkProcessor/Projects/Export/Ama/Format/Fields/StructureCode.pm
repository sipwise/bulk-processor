package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "STRUCUTRE CODE";

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name);
    my %params = @_;
    (
        $self->{name},
    ) = @params{qw/
        name
    /};

    return $self;

}

1;
