package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceObserved;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $NOT_OBSERVED_NOT_SAMPLED
    $OBSERVED_NOT_SAMPLED
    $NOT_OBSERVED_SAMPLED
    $OBSERVED_SAMPLED
    $SMDR_APPLICABLE
    $OBSERVED_NOT_SAMPLED_SMDR_APPLICABLE
    $NOT_OBSERVED_SAMPLED_SMDR_APPLICABLE
    $OBSERVED_SAMPLED_SMDR_APPLICABLE
);

my $field_name = "service observed";
my $length = 2;
my @param_names = qw/observed_sampled/;

my @observed_sampled_modes = ();
our $NOT_OBSERVED_NOT_SAMPLED = '0';
push(@observed_sampled_modes,$NOT_OBSERVED_NOT_SAMPLED);
our $OBSERVED_NOT_SAMPLED = '1';
push(@observed_sampled_modes,$OBSERVED_NOT_SAMPLED);
our $NOT_OBSERVED_SAMPLED = '2';
push(@observed_sampled_modes,$NOT_OBSERVED_SAMPLED);
our $OBSERVED_SAMPLED = '3';
push(@observed_sampled_modes,$OBSERVED_SAMPLED);
our $SMDR_APPLICABLE = '4';
push(@observed_sampled_modes,$SMDR_APPLICABLE);
our $OBSERVED_NOT_SAMPLED_SMDR_APPLICABLE = '5';
push(@observed_sampled_modes,$OBSERVED_NOT_SAMPLED_SMDR_APPLICABLE);
our $NOT_OBSERVED_SAMPLED_SMDR_APPLICABLE = '6';
push(@observed_sampled_modes,$NOT_OBSERVED_SAMPLED_SMDR_APPLICABLE);
our $OBSERVED_SAMPLED_SMDR_APPLICABLE = '7';
push(@observed_sampled_modes,$OBSERVED_SAMPLED_SMDR_APPLICABLE);

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{observed_sampled} //= $NOT_OBSERVED_NOT_SAMPLED;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($observed_sampled) = $self->_get_params(@_);
    die("invalid observed sampled '$observed_sampled'") unless contains($observed_sampled,\@observed_sampled_modes);
    return $observed_sampled . $TERMINATOR;

}

1;
