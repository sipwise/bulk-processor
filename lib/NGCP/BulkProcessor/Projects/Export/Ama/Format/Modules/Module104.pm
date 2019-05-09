package NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module104;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Module qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ModuleCode qw($MODULE_CODE_104);
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet NGCP::BulkProcessor::Projects::Export::Ama::Format::Module);
our @EXPORT_OK = qw(
    $length
);

our $length = 14;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Module->new(
        $class,
        length => $length,
        #structure_name => $structure_name,
        @_);
    $self->{module_code} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ModuleCode->new(
        module_code => $MODULE_CODE_104,
        @_,
    );
    $self->_add_field($self->{module_code});
    $self->_add_field(NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification->new(
        @_,
    ));

    return $self;

}

sub get_module_code_field {
    my $self = shift;
    return $self->{module_code};
}

#sub get_instance {
#    return
#}

1;