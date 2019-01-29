package NGCP::BulkProcessor::Projects::Export::Ama::Format::File;
use strict;

## no critic

use File::Basename qw(fileparse);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Settings qw(
    $output_path
    $ama_filename_format
    $use_tempfiles
    $tempfile_path
    $make_dir
    $ama_max_blocks
);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Block qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::LogError qw(
    fileerror
);

use NGCP::BulkProcessor::Utils qw(tempfilename makepath);
use NGCP::BulkProcessor::Calendar qw(current_local from_epoch);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

);

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
    $self->{tempfilename} = tempfilename('XXXX',$tempfile_path,$ama_file_extension) if $use_tempfiles;
    $self->{now} = current_local();
    $self->{min_start_time} = undef;
    $self->{max_end_time} = undef;
    return;
}

sub update_start_end_time {
    my $self = shift;
    my ($start_time,$end_time) = @_;
    #my $end_time = $start_time + $duration;
    $self->{min_start_time} = $start_time if (not defined $self->{min_start_time} or $self->{min_start_time} > $start_time);
    $self->{max_end_time} = $end_time if (not defined $self->{max_end_time} or $self->{max_end_time} < $end_time);
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
    if (not $pad and (scalar @{$self->{blocks}}) >= $ama_max_blocks and not $self->{current_block}->records_fit($record,
            $NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014::length)) {
        $result = 0;
    } else {
        if (not $self->{current_block}->add_record($record)) {
            if ((scalar @{$self->{blocks}}) >= $ama_max_blocks) {
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
    return $self->{tempfilename} if ($use_tempfiles and $show_tempfilename);
    return sprintf($ama_filename_format,
        $output_path,
        $self->{now}->year,
        substr($self->{now}->year,-2),
        $self->{now}->month,
        $self->{now}->day,
        $self->{now}->hour,
        $self->{now}->minute,
        $self->{now}->second,
        $self->{transfer_in}->get_structure()->get_file_sequence_number_field()->{file_sequence_number},
        $ama_file_extension,
    );
}

sub get_filesize {
    my $self = shift;
    return -s ($use_tempfiles ? $self->{tempfilename} : $self->get_filename());
}

sub _rename {
    my $self = shift;
    my ($filename) = @_;
    return rename($self->{tempfilename},$filename);
}

sub _makedir {

    my ($filename) = @_;
    my ($name,$path,$suffix) = fileparse($filename,$ama_file_extension);
    makepath($path,\&fileerror,getlogger(__PACKAGE__)) if ($make_dir and length($path) > 0 and not -d $path);
    return $filename;

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
    if ((scalar @{$self->{blocks}}) > 0 and (my $filename = $self->get_filename())) {
        if (-e $filename) {
            fileerror($filename . ' already exists',getlogger(__PACKAGE__));
            return 0;
        } else {
            if (open(my $fh,">:raw",($use_tempfiles ? $self->{tempfilename} : _makedir($filename)))) {
                foreach my $block (@{$self->{blocks}}) {
                    print $fh pack('H*',$block->get_hex());
                }
                close $fh;
                if (defined $commit_cb) {
                    if (&$commit_cb(@_)) {
                        if (not $use_tempfiles or $self->_rename(_makedir($filename))) {
                            return 1;
                        } else {
                            my $err = $!;
                            eval {
                                unlink $self->{tempfilename};
                            };
                            fileerror("failed to rename $self->{tempfilename} to $filename: $err",getlogger(__PACKAGE__));
                            return 0;
                        }
                    } else {
                        eval {
                            unlink $filename unless $use_tempfiles;
                            unlink $self->{tempfilename} if $use_tempfiles;
                        };
                        return 0;
                    }
                } else {
                    return 1;
                }
                #restdebug($self,"$self->{crt_path} saved",getlogger(__PACKAGE__));
            } else {
                fileerror('failed to open ' . ($use_tempfiles ? $self->{tempfilename} : $filename) . ": $!",getlogger(__PACKAGE__));
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

    # update transfer_in date/time:
    my $min_start_dt;
    $min_start_dt = from_epoch($self->{min_start_time}) if defined $self->{min_start_time};
    $self->{transfer_in}->get_structure()->get_date_field()->_set_params(
        date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($min_start_dt // $self->{now}),
    );
    $self->{transfer_in}->get_structure()->get_connect_time_field()->_set_params(
        connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($min_start_dt // $self->{now}),
    );
    # update transfer_out date/time:
    my $max_end_dt;
    $max_end_dt = from_epoch($self->{max_end_time}) if defined $self->{max_end_time};
    $self->{transfer_out}->get_structure()->get_date_field()->_set_params(
        date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($max_end_dt // $self->{now}),
    );
    $self->{transfer_out}->get_structure()->get_connect_time_field()->_set_params(
        connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($max_end_dt // $self->{now}),
    );
    # update transfer_out count fields:
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
