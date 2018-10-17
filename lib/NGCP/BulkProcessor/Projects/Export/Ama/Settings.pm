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

    check_dry

    $input_path
    $output_path

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $export_cdr_multithreading
    $export_cdr_blocksize
    $export_cdr_joins
    $export_cdr_conditions
    $export_cdr_limit
    $export_cdr_stream
);
#update_provider_config
#$deadlock_retries
#$generate_cdr_count

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

our $export_cdr_multithreading = $enablemultithreading;
our $export_cdr_blocksize = undef;
our $export_cdr_joins = [];
our $export_cdr_conditions = [];
our $export_cdr_limit = undef;
our $export_cdr_stream = undef;


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

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $export_cdr_multithreading = $data->{export_cdr_multithreading} if exists $data->{export_cdr_multithreading};
        $export_cdr_blocksize = $data->{export_cdr_blocksize} if exists $data->{export_cdr_blocksize};

        my $parse_result;
        ($parse_result,$export_cdr_joins) = _parse_export_joins($data->{export_cdr_joins},$configfile);
        ($parse_result,$export_cdr_conditions) = _parse_export_joins($data->{export_cdr_conditions},$configfile);

        $export_cdr_limit = $data->{export_cdr_limit} if exists $data->{export_cdr_limit};
        $export_cdr_stream = $data->{export_cdr_stream} if exists $data->{export_cdr_stream};

        #if ((confval("MAINTENANCE") // 'no') eq 'yes') {
        #        exit(0);
        #}

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

sub check_dry {

    if ($dry) {
        scriptinfo('running in dry mode - NGCP databases will not be modified',getlogger(__PACKAGE__));
        return 1;
    } else {
        scriptinfo('NO DRY MODE - NGCP DATABASES WILL BE MODIFIED!',getlogger(__PACKAGE__));
        if (!$force) {
            if ('yes' eq lc(prompt("Type 'yes' to proceed: "))) {
                return 1;
            } else {
                return 0;
            }
        } else {
            scriptinfo('force option applied',getlogger(__PACKAGE__));
            return 1;
        }
    }

}

1;
