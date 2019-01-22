package NGCP::BulkProcessor::Projects::Export::Ama::Format::Module;
use strict;

## no critic

use NGCP::BulkProcessor::LogError qw(
    notimplementederror
);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet);
our @EXPORT_OK = qw(

);

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet->new(
        $class,
        @_);
    my %params = @_;
    (
        $self->{enabled},
        $self->{module_instance},
    ) = @params{qw/
        enabled
        module_instance
    /};
    $self->{enabled} //= 1;

    return $self;

}

sub get_module_code_field {

    my $self = shift;
    #my (@params) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub get_name {
    my $self = shift;
    return $self->get_module_code_field()->get_module_code();
}

sub get_enabled {
    my $self = shift;
    return $self->{enabled};
}

1;
