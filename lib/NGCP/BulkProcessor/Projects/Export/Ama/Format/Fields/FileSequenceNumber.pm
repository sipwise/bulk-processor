package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $min_fsn
    $max_fsn
);

my $field_name = "file sequence number";
my $length = 4;
my @param_names = qw/file_sequence_number/;

our $min_fsn = 1;
our $max_fsn = 999;

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
    my ($file_sequence_number) = $self->_get_params(@_);
    die("invalid file sequence number '$file_sequence_number'") if ($file_sequence_number < $min_fsn or $file_sequence_number > $max_fsn);
    return sprintf('%03d',$file_sequence_number) . $TERMINATOR;

}

1;
