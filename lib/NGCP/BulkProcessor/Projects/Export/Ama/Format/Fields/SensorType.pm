package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorType;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $NOT_USED
    $DMS_100_FAMILY
);

my $field_name = "sensor type";
my $length = 4;
my @param_names = qw/is_error/;

our $NOT_USED = '000';
our $DMS_100_FAMILY = '036';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{sensor_type} //= $DMS_100_FAMILY;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($sensor_type) = $self->_get_params(@_);
    return $sensor_type . $TERMINATOR;

}

1;
