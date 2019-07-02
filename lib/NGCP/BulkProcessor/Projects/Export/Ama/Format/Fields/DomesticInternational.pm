package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Settings qw(
    $domestic_destination_pattern
    $international_destination_pattern
    $domestic_destination_not_pattern
    $international_destination_not_pattern
);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_number_domestic_international
);

my $field_name = "domestic/international indicator";
my $length = 2;
my @param_names = qw/domestic_international/;

my @domestic_international_modes = ();
our $DOMESTIC = '1';
push(@domestic_international_modes,$DOMESTIC);
our $INTERNATIONAL = '2';
push(@domestic_international_modes,$INTERNATIONAL);
our $UNKNOWN = '9';
push(@domestic_international_modes,$UNKNOWN);

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
    my ($domestic_international) = $self->_get_params(@_);
    die("invalid domestic international '$domestic_international'") unless contains($domestic_international,\@domestic_international_modes);
    return $domestic_international . $TERMINATOR;

}

sub get_number_domestic_international {
    my $number = shift; #called number (destination)
    if (defined $number) {
        if (defined $international_destination_pattern) {
            if ($number =~ $international_destination_pattern) {
                return $INTERNATIONAL;
            } else {
                return $DOMESTIC;
            }
        } elsif (defined $domestic_destination_pattern) {
            if ($number =~ $domestic_destination_pattern) {
                return $DOMESTIC;
            } else {
                return $INTERNATIONAL;
            }
        } elsif (defined $international_destination_not_pattern) {
            if ($number !~ $international_destination_not_pattern) {
                return $INTERNATIONAL;
            } else {
                return $DOMESTIC;
            }
        } elsif (defined $domestic_destination_not_pattern) {
            if ($number !~ $domestic_destination_not_pattern) {
                return $DOMESTIC;
            } else {
                return $INTERNATIONAL;
            }
        }
    }
    return $UNKNOWN;
}

1;