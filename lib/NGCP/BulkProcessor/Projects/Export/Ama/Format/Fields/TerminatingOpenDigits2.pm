package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TerminatingOpenDigits2;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "terminating open digits 2";
my $length = 10;
my @param_names = qw/terminating_open_digits_2/;

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
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($terminating_open_digits_2) = $self->_get_params(@_);
    if (length($terminating_open_digits_2) > 0) {
        die("invalid terminating open digits 2 '$terminating_open_digits_2'") unless $terminating_open_digits_2 =~ /^\d{1,9}$/;
        return sprintf('%09d',$terminating_open_digits_2) . $TERMINATOR;
    } else {
        return 'ffffffffff';
    }

}

1;
