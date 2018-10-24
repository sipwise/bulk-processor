package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $STRUCTURE_CODE_0510
    $STRUCTURE_CODE_0511
    $STRUCTURE_CODE_0512
    $STRUCTURE_CODE_0513
    $STRUCTURE_CODE_0514

    $STRUCTURE_CODE_9013
    $STRUCTURE_CODE_9014
);

my $field_name = "structure code";
my $length = 6;
my @param_names = qw/has_modules structure_code/;

my @structure_codes = ();
our $STRUCTURE_CODE_0510 = '0510';
push(@structure_codes,$STRUCTURE_CODE_0510);
our $STRUCTURE_CODE_0511 = '0511';
push(@structure_codes,$STRUCTURE_CODE_0511);
our $STRUCTURE_CODE_0512 = '0512';
push(@structure_codes,$STRUCTURE_CODE_0512);
our $STRUCTURE_CODE_0513 = '0513';
push(@structure_codes,$STRUCTURE_CODE_0513);
our $STRUCTURE_CODE_0514 = '0514';
push(@structure_codes,$STRUCTURE_CODE_0514);
our $STRUCTURE_CODE_9013 = '9013';
push(@structure_codes,$STRUCTURE_CODE_9013);
our $STRUCTURE_CODE_9014 = '9014';
push(@structure_codes,$STRUCTURE_CODE_9014);

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

sub set_has_modules {

    my $self = shift;
    $self->{has_modules} = shift;

}

sub get_structure_code {

    my $self = shift;
    return $self->{structure_code};

}

sub get_hex {

    my $self = shift;
    my ($has_modules,$structure_code) = $self->_get_params(@_);
    die("invalid structure code '$structure_code'") unless contains($structure_code,\@structure_codes);
    return ($has_modules ? '4' : '0') . $structure_code . $TERMINATOR;

}

1;
