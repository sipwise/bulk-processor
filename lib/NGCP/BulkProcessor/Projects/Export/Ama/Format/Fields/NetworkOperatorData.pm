package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::NetworkOperatorData;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_network_operator_data
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

sub get_network_operator_data {
    my ($originating_digits,$switch_number_digits) = @_;
    my $result = $originating_digits;
    my $padlength = 16 - length($originating_digits);
    $result .= 'f' x $padlength;
    $result .= $switch_number_digits;
    $padlength = 20 - length($switch_number_digits);
    $result .= 'f' x $padlength;
    $result .= '001';
    return $result;
}

1;
