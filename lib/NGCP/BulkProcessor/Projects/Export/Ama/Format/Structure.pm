package NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure;
use strict;

## no critic

use NGCP::BulkProcessor::LogError qw(
    notimplementederror
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

#my $base_structure = new NGCP::BulkProcessor::Projects::Export::Ama::Format::Structure(
#
#);

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;
    (
        $self->{structure_name},
        $self->{length},
    ) = @params{qw/
        structure_name
        length
    /};
    $self->{fields} = [];

    return $self;

}

sub _add_field {
    my $self = shift;
    push(@{$self->{fields}},@_);
}

sub get_name {
    my $self = shift;
    return $self->{name};
}

sub get_hex {

    my $self = shift;
    my $result = '';
    foreach my $field (@{$self->{fields}}) {
        $result .= $field->get_hex(@_);
    }
    return $result;
}

sub get_length {
    my $self = shift;
    return $self->{length} if defined $self->{length};
    my $length = 0;
    foreach my $field (@{$self->{fields}}) {
        $length += $field->get_length(@_);
    }
    return $length;
}

1;
