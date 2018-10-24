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
    my %params = @_;
    (
        $self->{name},
    ) = @params{qw/
        name
    /};

    return $self;

}

sub print {
    my $self = shift;


}

sub get_block_descriptor_word {
    my $self = shift;
    #return total length in bytes (up to 256*256 bytes)
}

sub pad {

}

1;
