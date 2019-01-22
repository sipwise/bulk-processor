package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::AdditionalDigitsDialed;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    
);

my $field_name = "digits string";
my $length = 16;
my @param_names = qw/additional_digits_dialed/;

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
    my ($additional_digits_dialed) = $self->_get_params(@_);
    die("invalid additional digits dialed '$additional_digits_dialed'") if (length($additional_digits_dialed) < 1 or length($additional_digits_dialed) > 15);
    return sprintf('%015d',$additional_digits_dialed) . $TERMINATOR;

}

1;
