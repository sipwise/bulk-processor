package NGCP::BulkProcessor::Projects::ETL::EDR::Settings;
use strict;

## no critic

use File::Basename qw(fileparse);

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

use NGCP::BulkProcessor::Utils qw(prompt timestampdigits threadid load_module);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    get_export_filename

    $ignore_period_events_unique
    $period_events_single_row_txn

    $sqlite_db_file
    $csv_dir

    check_dry

    $output_path
    $input_path

    $subscriber_profiles_export_filename_format

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $export_subscriber_profiles_multithreading
    $export_subscriber_profiles_numofthreads
    $export_subscriber_profiles_blocksize
    
    $export_subscriber_profiles_joins
    $export_subscriber_profiles_conditions
    $export_subscriber_profiles_limit
   
);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $ignore_period_events_unique = 0;
our $period_events_single_row_txn = 1;

our $output_path = $working_path . 'output/';
our $input_path = $working_path . 'input/';
our $csv_dir = 'events';

our $subscriber_profiles_export_filename_format = undef;

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

our $sqlite_db_file = 'sqlite';

our $export_subscriber_profiles_multithreading = $enablemultithreading;
our $export_subscriber_profiles_numofthreads = $cpucount;
our $export_subscriber_profiles_blocksize = 1000;

our $export_subscriber_profiles_joins = [];
our $export_subscriber_profiles_conditions = [];
our $export_subscriber_profiles_limit = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $subscriber_profiles_export_filename_format = $data->{subscriber_profiles_export_filename} if exists $data->{subscriber_profiles_export_filename};
        get_export_filename($data->{subscriber_profiles_export_filename},$configfile);
              
        $sqlite_db_file = $data->{sqlite_db_file} if exists $data->{sqlite_db_file};
        $csv_dir = $data->{csv_dir} if exists $data->{csv_dir};

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        
        my $parse_result;
        ($parse_result,$export_subscriber_profiles_joins) = _parse_export_joins($data->{export_subscriber_profiles_joins},$configfile);
        $result &= $parse_result;
        ($parse_result,$export_subscriber_profiles_conditions) = _parse_export_conditions($data->{export_subscriber_profiles_conditions},$configfile);
        $result &= $parse_result;
        
        $export_subscriber_profiles_limit = $data->{export_subscriber_profiles_limit} if exists $data->{export_subscriber_profiles_limit};

        $export_subscriber_profiles_multithreading = $data->{export_subscriber_profiles_multithreading} if exists $data->{export_subscriber_profiles_multithreading};
        $export_subscriber_profiles_numofthreads = _get_numofthreads($cpucount,$data,'export_subscriber_profiles_numofthreads');
        $export_subscriber_profiles_blocksize = $data->{export_subscriber_profiles_blocksize} if exists $data->{export_subscriber_profiles_blocksize};

        $period_events_single_row_txn = $data->{period_events_single_row_txn} if exists $data->{period_events_single_row_txn};
        $ignore_period_events_unique = $data->{ignore_period_events_unique} if exists $data->{ignore_period_events_unique};
        
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

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $numofthreads = $default_value;
    $numofthreads = $data->{$key} if exists $data->{$key};
    $numofthreads = $cpucount if $numofthreads > $cpucount;
    return $numofthreads;
}

sub get_export_filename {
    my ($filename_format,$configfile) = @_;
    my $export_filename;
    my $export_format;
    if ($filename_format) {
        $export_filename = sprintf($filename_format,timestampdigits(),threadid());
        unless ($export_filename =~ /^\//) {
            $export_filename = $output_path . $export_filename;
        }
        if (-e $export_filename and (unlink $export_filename) == 0) {
            filewarn('cannot remove ' . $export_filename . ': ' . $!,getlogger(__PACKAGE__));
            $export_filename = undef;
        }
        my ($name,$path,$suffix) = fileparse($export_filename,".csv");
        if ($suffix eq '.csv') {
            $export_format = 'csv';
        } else {
            configurationerror($configfile,"$filename_format: .csv export file format required");
        }
    }
    return ($export_filename,$export_format);
}

sub _parse_export_joins {
    my ($token,$file) = @_;
    my @joins = ();
    if (defined $token and length($token) > 0) {
        foreach my $f (_split(\$token)) {
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
        foreach my $f (_split(\$token)) {
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

1;
