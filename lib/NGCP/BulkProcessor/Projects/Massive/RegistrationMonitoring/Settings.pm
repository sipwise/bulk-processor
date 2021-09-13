package NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Settings;
use strict;

## no critic

use threads::shared qw();

use File::Basename qw(fileparse);
use DateTime::TimeZone qw();

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
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits threadid);
#format_number check_ipnet
use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    $sqlite_db_file

    $output_path
    $input_path
    $csv_dir

    $defaultsettings
    $defaultconfig

    $usernames_filename
    $usernames_rownum_start
    $load_registrations_numofthreads
    $load_registrations_multithreading
    $ignore_location_unique
    $location_single_row_txn
    
    $skip_errors
    $force

    get_export_filename
    $registrations_export_filename_format
);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $output_path = $working_path . 'output/';
our $input_path = $working_path . 'input/';
our $csv_dir = 'location';

our $usernames_filename = undef;
our $usernames_rownum_start = 0;
our $load_registrations_multithreading = $enablemultithreading;
our $load_registrations_numofthreads = $cpucount;
our $ignore_location_unique = 0;
our $location_single_row_txn = 0;

our $skip_errors = 0;
our $force = 0;

our $sqlite_db_file = 'sqlite';

our $registrations_export_filename_format = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $csv_dir = $data->{csv_dir} if exists $data->{csv_dir};
        $sqlite_db_file = $data->{sqlite_db_file} if exists $data->{sqlite_db_file};

        $load_registrations_multithreading = $data->{load_registrations_multithreading} if exists $data->{load_registrations_multithreading};
        $usernames_filename = _get_import_filename($usernames_filename,$data,'usernames_filename');
        $usernames_rownum_start = $data->{usernames_rownum_start} if exists $data->{usernames_rownum_start};
        $load_registrations_numofthreads = _get_numofthreads($cpucount,$data,'load_registrations_numofthreads');
        $ignore_location_unique = $data->{ignore_location_unique} if exists $data->{ignore_location_unique};
        $location_single_row_txn = $data->{location_single_row_txn} if exists $data->{location_single_row_txn};
        
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        $force = $data->{force} if exists $data->{force};

        $registrations_export_filename_format = $data->{registrations_export_filename} if exists $data->{registrations_export_filename};
        get_export_filename($data->{registrations_export_filename},$configfile);
        
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

sub _get_import_filename {
    my ($old_value,$data,$key) = @_;
    my $import_filename = $old_value;
    $import_filename = $data->{$key} if exists $data->{$key};
    if (defined $import_filename and length($import_filename) > 0) {
        $import_filename = $input_path . $import_filename unless -e $import_filename;
    }
    return $import_filename;
}

sub get_export_filename {
    my ($filename_format,$configfile) = @_;
    my $export_filename;
    my $export_format;
    if ($filename_format) {
        $export_filename = $output_path . sprintf($filename_format,timestampdigits(),threadid());
        if (-e $export_filename and (unlink $export_filename) == 0) {
            filewarn('cannot remove ' . $export_filename . ': ' . $!,getlogger(__PACKAGE__));
            $export_filename = undef;
        }
        my ($name,$path,$suffix) = fileparse($export_filename,".db",".csv");
        if ($suffix eq '.db') {
            $export_format = 'sqlite';
        } elsif ($suffix eq '.csv') {
            $export_format = 'csv';
        } else {
            configurationerror($configfile,"$filename_format: either .db or .csv export file format required");
        }
    }
    return ($export_filename,$export_format);
}

1;
