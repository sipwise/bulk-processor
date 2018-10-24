package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::OriginatingOpenDigits1;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "originating open digits 1";
my $length = 12;
my @param_names = qw/originating_open_digits_1/;

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
    my ($originating_open_digits_1) = $self->_get_params(@_);
    die("invalid originating open digits 1 '$originating_open_digits_1'") unless $originating_open_digits_1 =~ /^\d{1,11}$/;
    return sprintf('%011d',$originating_open_digits_1) . $TERMINATOR;

}

1;
