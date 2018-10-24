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



sub get_record_descriptor_word {
    my $self = shift;
    #return total length in bytes (up to 256*256 bytes)
}

1;
