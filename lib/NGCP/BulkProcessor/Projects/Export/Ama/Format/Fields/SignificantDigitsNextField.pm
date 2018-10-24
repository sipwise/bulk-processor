package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_number_length
    get_number_digits_1
    get_number_digits_2
);

my $field_name = "significant digits in next field";
my $length = 4;
#my @param_names = qw/significant_digits/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    #$self->{significant_digits} //= 0;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    if (defined $self->{instance}) {
        return ('significant_digits_' . $self->{instance});
    } else {
        return ('significant_digits');
    }

}

sub get_hex {

    my $self = shift;
    my ($significant_digits) = $self->_get_params(@_);
    die('invalid significant digits ' . (defined $self->{instance} ? $self->{instance} . ' ' : '') . "'$significant_digits'") if ($significant_digits < 0 or $significant_digits > 20);
    return sprintf('%03d',$significant_digits) . $TERMINATOR;

}

sub get_number_length {
    return length(shift);
}

sub get_number_digits_1 {
    return substr(shift,0,11);
}

sub get_number_digits_2 {
    return substr(shift,11);
}

1;
