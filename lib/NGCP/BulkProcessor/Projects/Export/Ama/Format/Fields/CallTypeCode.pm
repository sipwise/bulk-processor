package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallTypeCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $STATION_PAID
    $STATION_SPECIAL_CALLING
    $FLAT_RATE
    $FREE_CALL
    $OPERATING_COMPANY_NUMBER_SERVICE_CALLS
    $MDRRAO
    $ON_NET_PRIVATE_VIRTUAL_NETWORK
    $OFF_NET_PRIVATE_VIRTUAL_NETWORK
);

my $field_name = "call type code";
my $length = 4;
my @param_names = qw/call_type_code/;

our $STATION_PAID = '006';
our $STATION_SPECIAL_CALLING = '015';
our $FLAT_RATE = '067';
our $FREE_CALL = '074';
our $OPERATING_COMPANY_NUMBER_SERVICE_CALLS = '142';
our $MDRRAO = '159';
our $ON_NET_PRIVATE_VIRTUAL_NETWORK = '160';
our $OFF_NET_PRIVATE_VIRTUAL_NETWORK = '160';
#800-999 = Generic Record

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
    my ($call_type_code) = $self->_get_params(@_);
    die("invalid call type code '$call_type_code'") unless length($call_type_code) == 3;
    return $call_type_code . $TERMINATOR;

}

1;
