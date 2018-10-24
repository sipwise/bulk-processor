package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeType;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorType qw($NOT_USED $DMS_100_FAMILY);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "recording office type";
my $length = 4;
my @param_names = qw/recording_office_type/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{recording_office_type} //= $DMS_100_FAMILY;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($recording_office_type) = $self->_get_params(@_);
    die("invalid recording office type '$recording_office_type'") unless contains($recording_office_type,[$NOT_USED, $DMS_100_FAMILY]);
    return $recording_office_type . $TERMINATOR;

}

1;
