package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $STRUCTURE_CODE_0510
    $STRUCTURE_CODE_0511
    $STRUCTURE_CODE_0512
    $STRUCTURE_CODE_0513
    $STRUCTURE_CODE_0514
);

my $field_name = "structure code";
my $length = 6;
my @param_names = qw/has_modules structure_code/;

our $STRUCTURE_CODE_0510 = '0510';
our $STRUCTURE_CODE_0511 = '0511';
our $STRUCTURE_CODE_0512 = '0512';
our $STRUCTURE_CODE_0513 = '0513';
our $STRUCTURE_CODE_0514 = '0514';

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
    my ($has_modules,$structure_code) = $self->_get_params(@_);
    return ($has_modules ? '4' : '0') . $structure_code . $TERMINATOR;

}

1;
