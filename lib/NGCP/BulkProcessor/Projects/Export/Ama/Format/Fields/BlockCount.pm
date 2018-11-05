package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::BlockCount;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "block count";
my $length = 6;
my @param_names = qw/block_count/;

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
    my ($block_count) = $self->_get_params(@_);
    die("invalid block count '$block_count'") if ($block_count < 0 or $block_count > 99999);
    return sprintf('%05d',$block_count) . $TERMINATOR;

}

1;
