package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "hex id";
my $length = 2;
my @param_names = qw/is_error/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{is_error} //= 0;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($is_error) = $self->_get_params(@_);
    return ($is_error ? 'ab' : 'aa');

}

1;
