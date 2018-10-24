package NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet;
use strict;

## no critic

use NGCP::BulkProcessor::LogError qw(
    notimplementederror
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $field_separator = ": ";
my $line_terminator = "\n";
my $padding = ' ';

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = bless {}, $class;
    my %params = @_;
    (
        $self->{length},
    ) = @params{qw/
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
    #my (@params) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;
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

sub to_string {

    my $self = shift;
    my @lines = ();
    my $maxlen = 0;
    if (length($padding) > 0) {
        foreach my $field (@{$self->{fields}}) {
            $maxlen = length($field->get_name()) if $maxlen < length($field->get_name());
        }
    }
    foreach my $field (@{$self->{fields}}) {
        push(@lines,uc($field->get_name()) . $field_separator . ($maxlen > 0 ? ($padding x ($maxlen - length($field->get_name()))) : '') . uc($field->get_hex(@_)));
    }
    return join($line_terminator,@lines);

}

1;
