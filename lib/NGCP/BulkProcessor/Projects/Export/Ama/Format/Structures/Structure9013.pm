package NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode qw($STRUCTURE_CODE_9013);
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericIssue qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure);
our @EXPORT_OK = qw(

);

my $length = 64;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure->new(
        $class,
        length => $length,
        #structure_name => $structure_name,
        @_);
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId->new(
        @_,
    ));
    $self->{structure_code} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode->new(
        structure_code => $STRUCTURE_CODE_9013,
        @_,
    );
    $self->_add_field($self->{structure_code});
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType->new(
        call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::TRANSFER_IN,
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
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericIssue->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType->new(
        tracer_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType::TRANSFER_IN,
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber->new(
        @_,
    ));

    return $self;

}

sub get_structure_code_field {
    my $self = shift;
    return $self->{structure_code};
}

#sub get_instance {
#    return
#}

1;
