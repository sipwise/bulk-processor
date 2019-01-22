package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DataDescriptor;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "data descriptor";
my $length = 4;
my @param_names = qw/data_descriptor/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{data_descriptor} //= 1;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($data_descriptor) = $self->_get_params(@_);
    die("invalid data descriptor '$data_descriptor'") if (length($data_descriptor) < 1 or length($data_descriptor) > 3);
    return sprintf('%03d',$data_descriptor) . $TERMINATOR;

}

1;
