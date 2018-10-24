package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::OperatorAction;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "operator action";
my $length = 2;
my @param_names = ();

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
    #my ($unanswered) = $self->_get_params(@_);
    #die("invalid recording office type '$recording_office_type'") unless contains($recording_office_type,[$NOT_USED, $DMS_100_FAMILY]);
    return '0' . $TERMINATOR;

}

1;
