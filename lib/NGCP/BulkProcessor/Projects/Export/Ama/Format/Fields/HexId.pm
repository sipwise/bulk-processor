package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "HEX ID";
my $length = 2;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length);
    my %params = @_;
    (
        $self->{is_error},
    ) = @params{qw/
        is_error
    /};

    return $self;

}

sub get_hex {

    my $self = shift;
    return ($self->{is_error} ? 'ab' : 'aa');

}

1;
