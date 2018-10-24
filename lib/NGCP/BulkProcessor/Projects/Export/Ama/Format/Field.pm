package NGCP::BulkProcessor::Projects::Export::Ama::Format::Field;
use strict;

## no critic

use NGCP::BulkProcessor::LogError qw(
    notimplementederror
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    $TERMINATOR
);

our $TERMINATOR = 'c';

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;
    (
        $self->{name},
        $self->{length},
        $self->{instance},
    ) = @params{qw/
        name
        length
        instance
    /};
    $self->_set_params(@_);

    return $self;

}

sub _get_params {
    my $self = shift;
    my %params = @_;
    my @vals = ();
    foreach my $param_name ($self->_get_param_names()) {
        push(@vals,$params{$param_name} // $self->{$param_name});
    }
    return @vals;
}

sub _set_params {
    my $self = shift;
    my %params = @_;
    foreach my $param_name ($self->_get_param_names()) {
        $self->{$param_name} = $params{$param_name};
    }
}

sub get_name {
    my $self = shift;
    return $self->{name};
}

sub get_hex {

    my $self = shift;
    my (@params) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub _get_param_names {

    my $self = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub get_length {
    my $self = shift;
    return $self->{length} if defined $self->{length};
    return length($self->get_hex(@_));
}

1;
