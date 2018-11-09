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
    $self->{record_count} = 0;
}

sub add_record {
    my $self = shift;
    my ($record,$pad) = shift;
    my $result;
    if (not $self->{current_block}->add_record($record)) {
        if ((scalar @{$self->{blocks}}) >= $max_blocks) {
            $result = 0;
        } else {
            $self->{current_block}->set_padded(1);
            $self->{current_block} = NGCP::BulkProcessor::Projects::Export::Ama::Format::Block->new();
            push(@{$self->{blocks}},$self->{current_block});
            $result = $self->{current_block}->add_record($record);
            $self->{record_count} += 1;
        }
    } else {
        $self->{record_count} += 1;
        $self->{current_block}->set_padded(1) if $pad;
        $result = 1;
    }
    return $result;

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
        return 1;
    } else {
        #fileerror("failed to open $self->{crt_path}: $!",getlogger(__PACKAGE__));
        return 0;
    }
}

sub write_record {

    my $self = shift;
    my %params = @_;
    (
        $get_transfer_in,
        $get_record,
        $get_transfer_out,
        $close,
    ) = @params{qw/
        get_transfer_in
        get_record
        get_transfer_out
        close
    /};

    $file->add_record(
        &$get_transfer_in(
            file_sequence_number => 1,
            @_,
        ),
        1
    ) unless $self->{record_count} > 0;

    my $result = 0;
    my $record = &$get_record();
    if (not $file->add_record($record)) {
        $file->add_record(
            &$get_transfer_out(

                file_sequence_number => 1,

                => (scalar @records),
                @_
            ),
            1,
        );
        $result |= $file->flush();
        $file->add_record(
            &$get_transfer_in(

                file_sequence_number => 1,
                @_
            ),
            1
        );

        $file->add_record($record)
    }

    if ($close) {
        $file->add_record(
            &$get_transfer_out(

                file_sequence_number => 1,

                => (scalar @records),
                @_
            ),
            1,
        );
        $result |= $file->flush();
    }
    return $result;

}

1;
