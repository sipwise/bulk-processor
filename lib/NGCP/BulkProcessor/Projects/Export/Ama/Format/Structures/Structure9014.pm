package NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::HexId qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode qw($STRUCTURE_CODE_9014);
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
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordCount qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::BlockCount qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure);
our @EXPORT_OK = qw(
    $length
);

our $length = 78;

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
        structure_code => $STRUCTURE_CODE_9014,
        @_,
    );
    $self->_add_field($self->{structure_code});
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType->new(
        call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::TRANSFER,
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
    $self->{date} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date->new(
        @_,
    );
    $self->_add_field($self->{date});
    $self->{connect_time} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime->new(
        @_,
    );
    $self->_add_field($self->{connect_time});
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericIssue->new(
        @_,
    ));
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType->new(
        tracer_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TracerType::TRANSFER_OUT,
        @_,
    ));
    $self->{file_sequence_number} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber->new(
        @_,
    );
    $self->_add_field($self->{file_sequence_number});

    $self->{record_count} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordCount->new(
        @_,
    );
    $self->_add_field($self->{record_count});

    $self->{block_count} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::BlockCount->new(
        @_,
    );
    $self->_add_field($self->{block_count});

    return $self;

}

sub get_structure_code_field {
    my $self = shift;
    return $self->{structure_code};
}

sub get_file_sequence_number_field {
    my $self = shift;
    return $self->{file_sequence_number};
}

sub get_record_count_field {
    my $self = shift;
    return $self->{record_count};
}

sub get_block_count_field {
    my $self = shift;
    return $self->{block_count};
}

sub get_date_field {
    my $self = shift;
    return $self->{date};
}

sub get_connect_time_field {
    my $self = shift;
    return $self->{connect_time};
}

#sub get_instance {
#    return
#}

1;
