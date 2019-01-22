package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $ADDITIONAL_BILLING
    $ISUP_CARRIER
    $ADDITIONAL_PARTY_CATEGORY
    $IN_CORRELATION_ID
    $CHARGE_AREA
    $FORWARD_FACILITY_COUNT
    $BACKWARD_FACILITY_COUNT
    $FTUP_BACKWARD_CHARGE
    $CLI_SCREENING
    $ADDITIONAL_CALLING
    $NUMBER_PORTABILITY
    $NPI
    $CPS
);

my $field_name = "generic context identifier";
my $length = 8;
my @param_names = qw/generic_context_identifier parsing_rules/;

my @generic_context_ids = ();
our $ADDITIONAL_BILLING = '80005';
push(@generic_context_ids,$ADDITIONAL_BILLING);
our $ISUP_CARRIER = '80006';
push(@generic_context_ids,$ISUP_CARRIER);
our $ADDITIONAL_PARTY_CATEGORY = '80008';
push(@generic_context_ids,$ADDITIONAL_PARTY_CATEGORY);
our $IN_CORRELATION_ID = '80014';
push(@generic_context_ids,$IN_CORRELATION_ID);
our $CHARGE_AREA = '80016';
push(@generic_context_ids,$CHARGE_AREA);
our $FORWARD_FACILITY_COUNT = '80021';
push(@generic_context_ids,$FORWARD_FACILITY_COUNT);
our $BACKWARD_FACILITY_COUNT = '80022';
push(@generic_context_ids,$BACKWARD_FACILITY_COUNT);
our $FTUP_BACKWARD_CHARGE = '80025';
push(@generic_context_ids,$FTUP_BACKWARD_CHARGE);
our $CLI_SCREENING = '80026';
push(@generic_context_ids,$CLI_SCREENING);
our $ADDITIONAL_CALLING = '80027';
push(@generic_context_ids,$ADDITIONAL_CALLING);
our $NUMBER_PORTABILITY = '80030';
push(@generic_context_ids,$NUMBER_PORTABILITY);
our $NPI = '80050';
push(@generic_context_ids,$NPI);
our $CPS = '80080';
push(@generic_context_ids,$CPS);

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{parsing_rules} //= 0;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($generic_context_identifier,$parsing_rules) = $self->_get_params(@_);
    die("invalid generic context identifier '$generic_context_identifier'") unless contains($generic_context_identifier,\@generic_context_ids);
    die("invalid parsing rules '$parsing_rules'") if (length($parsing_rules) < 1 or length($parsing_rules) > 2);
    return $generic_context_identifier . sprintf('%02d',$parsing_rules) . $TERMINATOR;

}

1;
