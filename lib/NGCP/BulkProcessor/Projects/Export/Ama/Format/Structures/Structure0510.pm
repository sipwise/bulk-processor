package NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode qw($STRUCTURE_CODE_0510);
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallTypeCode qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TimingIndicator qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StudyIndicator qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure);
our @EXPORT_OK = qw(

);

my $structure_name = "call type code";

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure->new(
        $class,
        structure_name => $structure_name,
        @_);
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode->new(
        structure_code => $STRUCTURE_CODE_0510,
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallTypeCode->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorType->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorId->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeType->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeId->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TimingIndicator->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StudyIndicator->new(
        @_,
    ));
    return $self;

}

1;
