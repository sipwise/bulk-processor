package NGCP::BulkProcessor::Projects::Export::Ama::Settings;
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
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits);
#format_number check_ipnet

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    $input_path
    $output_path

    $defaultsettings
    $defaultconfig


    $skip_errors
    $force

    $export_cdr_multithreading
    $export_cdr_numofthreads
    $export_cdr_blocksize
    $export_cdr_joins
    $export_cdr_conditions
    $export_cdr_limit
    $export_cdr_stream

    $domestic_destination_pattern
    $international_destination_pattern

    $ama_filename_format
);
#check_dry
#$dry
#update_provider_config
#$deadlock_retries
#$generate_cdr_count

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';

our $force = 0;
#our $dry = 0;
our $skip_errors = 0;

our $export_cdr_multithreading = $enablemultithreading;
our $export_cdr_numofthreads = $cpucount;
our $export_cdr_blocksize = undef;
our $export_cdr_joins = [];
our $export_cdr_conditions = [];
our $export_cdr_limit = undef;
our $export_cdr_stream = undef;

our $domestic_destination_pattern = undef;
our $international_destination_pattern = undef;

our $ama_filename_format = '%1$s%2$s.ama';

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);
        #if ($data->{report_filename}) {
        #    $report_filename = $output_path . sprintf('/' . $data->{report_filename},timestampdigits());
        #    if (-e $report_filename and (unlink $report_filename) == 0) {
        #        filewarn('cannot remove ' . $report_filename . ': ' . $!,getlogger(__PACKAGE__));
        #        $report_filename = undef;
        #    }
        #} else {
        #    $report_filename = undef;
        #}

        #$dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $export_cdr_multithreading = $data->{export_cdr_multithreading} if exists $data->{export_cdr_multithreading};
        $export_cdr_numofthreads = _get_numofthreads($cpucount,$data,'export_cdr_numofthreads');
        $export_cdr_blocksize = $data->{export_cdr_blocksize} if exists $data->{export_cdr_blocksize};

        my $parse_result;
        ($parse_result,$export_cdr_joins) = _parse_export_joins($data->{export_cdr_joins},$configfile);
        $result &= $parse_result;
        ($parse_result,$export_cdr_conditions) = _parse_export_joins($data->{export_cdr_conditions},$configfile);
        $result &= $parse_result;

        $export_cdr_limit = $data->{export_cdr_limit} if exists $data->{export_cdr_limit};
        $export_cdr_stream = $data->{export_cdr_stream} if exists $data->{export_cdr_stream};

        #if ((confval("MAINTENANCE") // 'no') eq 'yes') {
        #        exit(0);
        #}
        my $regexp_result;
        $domestic_destination_pattern = $data->{domestic_destination_pattern} if exists $data->{domestic_destination_pattern};
        ($regexp_result,$domestic_destination_pattern) = parse_regexp($domestic_destination_pattern,$configfile);
        $result &= $regexp_result;
        $international_destination_pattern = $data->{international_destination_pattern} if exists $data->{international_destination_pattern};
        ($regexp_result,$international_destination_pattern) = parse_regexp($international_destination_pattern,$configfile);
        $result &= $regexp_result;

        $ama_filename_format = $data->{ama_filename_format} if exists $data->{ama_filename_format};

        return $result;

    }
    return 0;

}

sub _prepare_working_paths {

    my ($create) = @_;
    my $result = 1;
    my $path_result;

    ($path_result,$input_path) = create_path($working_path . 'input',$input_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;
    ($path_result,$output_path) = create_path($working_path . 'output',$output_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

sub _parse_export_joins {
    my ($token,$file) = @_;
    my @joins = ();
    if (defined $token and length($token) > 0) {
        foreach my $f (split_tuple($token)) {
            next unless($f);
            $f =~ s/^\s*\{?\s*//;
            $f =~ s/\}\s*\}\s*$/}/;
            my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
            $a =~ s/^\s*\'//;
            $a =~ s/\'$//g;
            $b =~ s/\s*\}\s*$//;
            my ($c, $d) = split(/\s*=>\s*/, $b);
            $c =~ s/^\s*\'//g;
            $c =~ s/\'\s*//;
            $d =~ s/^\s*\'//g;
            $d =~ s/\'\s*//;
            push @joins, { $a => { $c => $d } };
        }
    }
    return (1,\@joins);
}

sub _parse_export_conditions {
    my ($token,$file) = @_;
    my @conditions = ();
    if (defined $token and length($token) > 0) {
        foreach my $f (split_tuple($token)) {
            next unless($f);
            $f =~ s/^\s*\{?\s*//;
            $f =~ s/\}\s*\}\s*$/}/;
            my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
            $a =~ s/^\s*\'//;
            $a =~ s/\'$//g;
            $b =~ s/\s*\}\s*$//;
            my ($c, $d) = split(/\s*=>\s*/, $b);
            $c =~ s/^\s*\'//g;
            $c =~ s/\'\s*//;
            $d =~ s/^\s*\'//g;
            $d =~ s/\'\s*//;
            push @conditions, { $a => { $c => $d } };
        }
    }
    return (1,\@conditions);
}

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $_numofthreads = $default_value;
    $_numofthreads = $data->{$key} if exists $data->{$key};
    $_numofthreads = $cpucount if $_numofthreads > $cpucount;
    return $_numofthreads;
}

1;
