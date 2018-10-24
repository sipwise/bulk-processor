package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::OriginatingChargeInformation;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "originating charge information";
my $length = 4;
my @param_names = qw/originating_charge_information/;

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
    my ($originating_charge_information) = $self->_get_params(@_);
    if (length($originating_charge_information) > 0) {
        die("invalid originating charge information '$originating_charge_information'") unless length($originating_charge_information) == 3; #($originating_charge_information < 0 or $originating_charge_information > 255);
        return $originating_charge_information . $TERMINATOR;
    } else {
        return 'ffff';
    }

}

1;
