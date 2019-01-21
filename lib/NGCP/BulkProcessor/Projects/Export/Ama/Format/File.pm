package NGCP::BulkProcessor::Projects::Export::Ama::Format::File;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Settings qw(
    $output_path
    $ama_filename_format
    $export_cdr_use_temp_files
    $tempfile_path
);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Block qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014 qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::LogError qw(
    fileerror
);

use NGCP::BulkProcessor::Utils qw(tempfilename);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

my $max_blocks = 99;
my $ama_file_extension = '.ama';

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
    $self->_save_transfer_in(undef);
    $self->_save_transfer_out(undef);
    $self->{tempfilename} = tempfilename('XXXX',$tempfile_path,$ama_file_extension) if $export_cdr_use_temp_files;
    return;
}

sub get_record_count {
    my $self = shift;
    return $self->{record_count};
}

sub get_block_count {
    my $self = shift;
    return scalar @{$self->{blocks}};
}

sub add_record {
    my $self = shift;
    my ($record,$pad) = @_;
    my $result;
    if (not $pad and (scalar @{$self->{blocks}}) >= $max_blocks and not $self->{current_block}->records_fit($record,
            $NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014::length)) {
        $result = 0;
    } else {
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
    }
    return $result;

}

sub get_filename {
    my $self = shift;
    my ($show_tempfilename) = @_;
    return $self->{tempfilename} if ($export_cdr_use_temp_files and $show_tempfilename);
    return sprintf($ama_filename_format,
        $output_path,
        $self->{transfer_in}->get_structure()->get_file_sequence_number_field()->{file_sequence_number},
        $ama_file_extension,
    );
}

sub get_filesize {
    my $self = shift;
    return -s ($export_cdr_use_temp_files ? $self->{tempfilename} : $self->get_filename());
}

sub _rename {
    my $self = shift;
    my ($filename) = @_;
    return rename($self->{tempfilename},$filename);
}

sub flush {
    my $self = shift;
    my %params = @_;
    my (
        $commit_cb,
    ) = @params{qw/
        commit_cb
    /};
    #unlink 'test.ama';
    if ((scalar @{$self->{blocks}}) > 0 and (my $filename = ($export_cdr_use_temp_files ? $self->{tempfilename} : $self->get_filename()))) {
        if (not $export_cdr_use_temp_files and -e $filename) {
            fileerror($filename . ' already exists',getlogger(__PACKAGE__));
            return 0;
        } else {
            if (open(my $fh,">:raw",$filename)) {
                foreach my $block (@{$self->{blocks}}) {
                    print $fh pack('H*',$block->get_hex());
                }
                close $fh;
                if (defined $commit_cb) {
                    if (&$commit_cb(@_) and (not $export_cdr_use_temp_files or $self->_rename($filename))) {
                        return 1;
                    } else {
                        eval {
                            unlink $filename unless $export_cdr_use_temp_files;
                            unlink $self->{tempfilename} if $export_cdr_use_temp_files;
                        };
                        return 0;
                    }
                } else {
                    return 1;
                }
                #restdebug($self,"$self->{crt_path} saved",getlogger(__PACKAGE__));
            } else {
                fileerror('failed to open ' . $filename . ": $!",getlogger(__PACKAGE__));
                return 0;
            }
        }
    } else {
        return 0;
    }
}

sub close {

    my $self = shift;
    my %params = @_;
    my (
        $get_transfer_out,
        $commit_cb,
    ) = @params{qw/
        get_transfer_out
        commit_cb
    /};
    my $result = 0;
    $self->add_record(
        $self->_save_transfer_out(&$get_transfer_out(
            #file_sequence_number => 1,

            #=> (scalar @records),
            @_
        )),
        1,
    );
    # update count fields:
    $self->{transfer_out}->get_structure()->get_block_count_field()->_set_params(block_count => $self->get_block_count());
    $self->{transfer_out}->get_structure()->get_record_count_field()->_set_params(record_count => $self->get_record_count());
    $result |= $self->flush(
        commit_cb => $commit_cb,
        @_
    );
    $self->reset();
    return $result;

}

sub write_record {

    my $self = shift;
    my %params = @_;
    my (
        $get_transfer_in,
        $get_record,
        $get_transfer_out,
        $commit_cb,
    ) = @params{qw/
        get_transfer_in
        get_record
        get_transfer_out
        commit_cb
    /};

    $self->add_record(
        $self->_save_transfer_in(&$get_transfer_in(
            #file_sequence_number => 1,
            @_,
        )),
        1
    ) unless $self->{record_count} > 0;

    my $result = 0;
    my $record = &$get_record(@_);
    if (not $self->add_record($record)) {
        #my $blah="y";
        $result |= $self->close(
            get_transfer_out => $get_transfer_out,
            commit_cb => $commit_cb,
            @_
        );
        $self->add_record(
            $self->_save_transfer_in(&$get_transfer_in(

                #file_sequence_number => 1,
                @_
            )),
            1
        );

        $self->add_record($record);
    }

    return $result;

}

sub _save_transfer_in {
    my $self = shift;
    my $record = shift;
    if (defined $record) {
        $self->{transfer_in} = $record;
    } else {
        undef $self->{transfer_in};
    }
    return $record;
}

sub _save_transfer_out {
    my $self = shift;
    my $record = shift;
    if (defined $record) {
        $self->{transfer_out} = $record;
    } else {
        undef $self->{transfer_out};
    }
    return $record;
}

1;
