package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::NetworkOperatorData;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "network operator data";
my $length = 49;
my @param_names = qw/network_operator_data/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($network_operator_data) = $self->_get_params(@_);
    die("invalid network operator data '$network_operator_data'") if length($network_operator_data) != 39;
    return $network_operator_data . $TERMINATOR;

}

1;
