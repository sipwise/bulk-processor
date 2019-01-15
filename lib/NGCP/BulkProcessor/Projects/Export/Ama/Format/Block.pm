package NGCP::BulkProcessor::Projects::Export::Ama::Format::Block;
use strict;

## no critic

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $block_descriptor_word_length = 8;
my $max_block_length = 2048; #bytes

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    $self->{records} = [];
    $self->{padded} = 0;
    $self->{length} = $block_descriptor_word_length;

    return $self;

}

sub add_record {
    my $self = shift;
    my $record = shift;
    my $length;
    if (not $self->{padded} and ($length = $self->get_length() + $record->get_length()) <= 2 * $max_block_length) {
        push(@{$self->{records}},$record);
        $self->{length} = $length;
        return 1;
    }
    return 0;

}

sub records_fit {
    my $self = shift;
    my @records = @_;
    if (not $self->{padded}) {
        my $length = $self->get_length();
        foreach my $record (@records) {
            if (not ref $record) {
                $length += $record;
            } else {
                $length += $record->get_length();
            }
        }
        if ($length <= 2 * $max_block_length) {
            return 1;
        }
    }
    return 0;

}

sub get_hex {

    my $self = shift;
    my $result = $self->_get_block_descriptor_word();
    foreach my $record (@{$self->{records}}) {
        $result .= $record->get_hex();
    }
    if ($self->{padded}) {
        $result .= 'a' x (2 * $max_block_length - length($result));
    }
    return $result;
}


sub get_length {
    my $self = shift;
    return $self->{length};
}


sub _get_block_descriptor_word {
    my $self = shift;
    return sprintf('%04x',$self->get_length() / 2) . '0000';
    #return total length in bytes (up to 256*256 bytes)
}

sub set_padded {
    my $self = shift;
    $self->{padded} = shift;
}

1;
