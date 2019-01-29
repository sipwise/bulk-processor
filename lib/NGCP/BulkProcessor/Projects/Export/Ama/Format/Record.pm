package NGCP::BulkProcessor::Projects::Export::Ama::Format::Record;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::FieldSet qw($line_terminator);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $record_descriptor_word_length = 8;

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    $self->{structure} = shift;
    $self->{modules} = [ @_ ];
    $self->{structure}->get_structure_code_field()->set_has_modules((scalar @{$self->{modules}}) > 0);

    return $self;

}

sub get_structure {
    my $self = shift;
    return $self->{structure};
}

sub get_hex {

    my $self = shift;
    my $result = $self->_get_record_descriptor_word(@_);
    $result .= $self->{structure}->get_hex(@_);
    foreach my $module (@{$self->{modules}}) {
        next unless $module->get_enabled(@_);
        $result .= $module->get_hex(@_);
    }
    return $result;
}

sub to_string {

    my $self = shift;

    my $result = $line_terminator . $line_terminator . "record data:$line_terminator" . $self->{structure}->to_string(@_);
    foreach my $module (@{$self->{modules}}) {
        next unless $module->get_enabled(@_);
        $result .= $line_terminator . $line_terminator . "module data:$line_terminator" . $module->to_string(@_);
    }
    $result .= $line_terminator;
    return $result;

}

sub get_length {
    my $self = shift;
    my $length = $record_descriptor_word_length;
    $length += $self->{structure}->get_length(@_);
    foreach my $module (@{$self->{modules}}) {
        next unless $module->get_enabled(@_);
        $length += $module->get_length(@_);
    }
    return $length;
}


sub _get_record_descriptor_word {
    my $self = shift;
    return sprintf('%04x',$self->get_length(@_) / 2) . '0000';
    #return total length in bytes (up to 256*256 bytes)
}

1;
