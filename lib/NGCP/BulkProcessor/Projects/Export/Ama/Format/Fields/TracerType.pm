package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $TRANSFER_IN
    $TRANSFER_OUT
    $AMATPS_PRIMARY
    $AMATPS_SECONDARY
    $DMS100F_HOURLY
);

my $field_name = "tracer type";
my $length = 4;
my @param_names = qw/tracer_type/;

my @tracer_types = ();
our $TRANSFER_IN = '007';
push(@tracer_types,$TRANSFER_IN);
our $TRANSFER_OUT = '008';
push(@tracer_types,$TRANSFER_OUT);
our $AMATPS_PRIMARY = '032';
push(@tracer_types,$AMATPS_PRIMARY);
our $AMATPS_SECONDARY = '033';
push(@tracer_types,$AMATPS_SECONDARY);
our $DMS100F_HOURLY = '037';
push(@tracer_types,$DMS100F_HOURLY);

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
    my ($tracer_type) = $self->_get_params(@_);
    die("invalid tracer type '$tracer_type'") unless contains($tracer_type,\@tracer_types);
    return $tracer_type . $TERMINATOR;

}

1;
