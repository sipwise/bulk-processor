package NGCP::BulkProcessor::Projects::Export::Ama::Format::Settings;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $working_path
    $enablemultithreading
    $cpucount
    create_path
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    scriptinfo
    configurationinfo
);

use NGCP::BulkProcessor::LogError qw(
    fileerror
    filewarn
    configurationwarn
    configurationerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    split_tuple
    parse_regexp
);
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits); #stringtobool
#format_number check_ipnet

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    $output_path
    $tempfile_path

    $domestic_destination_pattern
    $international_destination_pattern

    $make_dir
    $ama_filename_format
    $use_tempfiles

    $ama_max_blocks
);

our $output_path = $working_path . 'output/';
our $tempfile_path = $working_path . 'temp/';

our $use_tempfiles = 0;

our $domestic_destination_pattern = undef;
our $international_destination_pattern = undef;

our $ama_max_blocks = 1000;

our $make_dir = 0;
our $ama_filename_format = '%1$sP%3$02d%4$02d%5$02d%6$02d%7$02d%9$02dAMA%10$s';

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $use_tempfiles = $data->{use_tempfiles} if exists $data->{use_tempfiles};
        $make_dir = $data->{make_dir} if exists $data->{make_dir};

        my $regexp_result;
        $domestic_destination_pattern = $data->{domestic_destination_pattern} if exists $data->{domestic_destination_pattern};
        ($regexp_result,$domestic_destination_pattern) = parse_regexp($domestic_destination_pattern,$configfile);
        $result &= $regexp_result;
        $international_destination_pattern = $data->{international_destination_pattern} if exists $data->{international_destination_pattern};
        ($regexp_result,$international_destination_pattern) = parse_regexp($international_destination_pattern,$configfile);
        $result &= $regexp_result;

        $ama_filename_format = $data->{ama_filename_format} if exists $data->{ama_filename_format};

        $ama_max_blocks = $data->{ama_max_blocks} if exists $data->{ama_max_blocks};

        return $result;

    }
    return 0;

}

sub _prepare_working_paths {

    my ($create) = @_;
    my $result = 1;
    my $path_result;

    ($path_result,$output_path) = create_path($working_path . 'output',$output_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;
    ($path_result,$tempfile_path) = create_path($working_path . 'temp',$output_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

1;
