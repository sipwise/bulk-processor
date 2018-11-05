package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordCount;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "record count";
my $length = 8;
my @param_names = qw/record_count/;

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
    my ($record_count) = $self->_get_params(@_);
    die("invalid record count '$record_count'") if ($record_count < 0 or $record_count > 9999999);
    return sprintf('%07d',$record_count) . $TERMINATOR;

}

1;
