package NGCP::BulkProcessor::Projects::Export::Ama::Format::Field;
use strict;

## no critic

use NGCP::BulkProcessor::LogError qw(
    notimplementederror
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;
    (
        $self->{name},
        $self->{length},
    ) = @params{qw/
        name
        length
    /};

    return $self;

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

#sub get_bytes {
#    my $self = shift;
#    return pack('H*', $self->get_hex(@_));
#}

sub get_length {
    my $self = shift;
    return $self->{length} if defined $self->{length};
    return length($self->get_hex(@_));
}

1;
