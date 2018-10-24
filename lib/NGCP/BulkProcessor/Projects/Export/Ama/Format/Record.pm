package NGCP::BulkProcessor::Projects::Export::Ama::Format::Record;
use strict;

## no critic

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    $self->{structure} = shift;
    $self->{modules} = [ @_ ];
    $self->{structure}->get_structure_code_field()->set_has_modules((scalar @{$self->{modules}}) > 0);

    return $self;

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

sub get_length {
    my $self = shift;
    my $length = 8; #RDW
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
