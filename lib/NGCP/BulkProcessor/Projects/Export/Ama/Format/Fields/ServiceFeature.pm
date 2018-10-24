package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $OTHER
    $THREE_WAY
    $CF_LEG
    $CFB_LEG
    $BTUP_CBWF
    $ROUTE_OPT_IND
);

my $field_name = "service feature";
my $length = 4;
my @param_names = qw/service_feature_code/;

our $OTHER = '000';
our $THREE_WAY = '010';
our $CF_LEG = '012';
our $CFB_LEG = '014';
our $BTUP_CBWF = '029';
our $ROUTE_OPT_IND = '156';
#800-999 = generic value

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    #$self->{service_feature_code} //= 0;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($service_feature_code) = $self->_get_params(@_);
    die("invalid service feature code '$service_feature_code'") unless length($service_feature_code) == 3;
    return $service_feature_code . $TERMINATOR;

}

1;
