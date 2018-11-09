package NGCP::BulkProcessor::Projects::Export::Ama::Format::File;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Block qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $max_blocks = 99;

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    $self->reset();

    return $self;

}

sub reset {
    my $self = shift;
    $self->{current_block} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Block->new();
    $self->{blocks} = [ $self->{current_block} ];
}

sub add_record {
    my $self = shift;
    my ($record,$pad) = shift;
    if (not $self->{current_block}->add_record($record)) {
        if ((scalar @{$self->{blocks}}) >= $max_blocks) {
            return 0;
        } else {
            $self->{current_block}->set_padded(1);
            $self->{current_block} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Block->new();
            push(@{$self->{blocks}},$self->{current_block});
            return $self->{current_block}->add_record($record);
        }
    } else {
        $self->{current_block}->set_padded(1) if $pad;
        return 1;
    }

}

sub flush {
    my $self = shift;
    #unlink 'test.ama';
    if (open(my $fh,">:raw",'test.ama')) {
        foreach my $block (@{$self->{blocks}}) {
            print $fh pack('H*',$block->get_hex());
        }
        close $fh;
        $self->reset();
        #restdebug($self,"$self->{crt_path} saved",getlogger(__PACKAGE__));
    } else {
        #fileerror("failed to open $self->{crt_path}: $!",getlogger(__PACKAGE__));
    }
}

1;
