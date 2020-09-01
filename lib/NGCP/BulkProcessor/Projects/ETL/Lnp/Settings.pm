package NGCP::BulkProcessor::Projects::ETL::Lnp::Settings;
use strict;

## no critic

use threads::shared qw();

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
use NGCP::BulkProcessor::Utils qw(prompt);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    $sqlite_db_file

    check_dry
    
    $input_path

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $import_multithreading

    $lnp_filename
    $lnp_rownum_start
    $lnp_import_numofthreads
    $ignore_lnp_unique
    $lnp_import_single_row_txn

    $expand_numbers_code
    
    $create_lnp_multithreading
    $create_lnp_numofthreads
    
    $delete_lnp_multithreading
    $delete_lnp_numofthreads
    
    $ignore_lnp_numbers_unique
    $lnp_numbers_single_row_txn
    
    $lnp_numbers_batch_delete
);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.yml';

our $input_path = $working_path . 'input/';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

our $sqlite_db_file = 'sqlite';

our $import_multithreading = 1;

our $lnp_filename = undef;
our $lnp_rownum_start = 2;
our $lnp_import_numofthreads = $cpucount;
our $ignore_lnp_unique = 0;
our $lnp_import_single_row_txn = 0;
our $expand_numbers_code = undef;

our $create_lnp_multithreading = 1;
our $create_lnp_numofthreads = $cpucount;
    
our $delete_lnp_multithreading = 1;
our $delete_lnp_numofthreads = $cpucount;

our $ignore_lnp_numbers_unique = 0;
our $lnp_numbers_single_row_txn = 0;

our $lnp_numbers_batch_delete = 1;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $sqlite_db_file = $data->{sqlite_db_file} if exists $data->{sqlite_db_file};

        $lnp_filename = _get_import_filename($lnp_filename,$data,'lnp_filename');
        unless ($lnp_filename and -e $lnp_filename) {
            configurationerror($configfile,"invalid lnp filename",getlogger(__PACKAGE__));
        }
        $lnp_rownum_start = $data->{lnp_rownum_start} if exists $data->{lnp_rownum_start};
        $lnp_import_single_row_txn = $data->{lnp_import_single_row_txn} if exists $data->{lnp_import_single_row_txn};
        $ignore_lnp_unique = $data->{ignore_lnp_unique} if exists $data->{ignore_lnp_unique};
        
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};
        $lnp_import_numofthreads = _get_numofthreads($lnp_import_numofthreads,$data,'lnp_import_numofthreads');

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        
        $expand_numbers_code = $data->{expand_numbers} if exists $data->{expand_numbers};
        if (defined $expand_numbers_code and 'CODE' ne ref $expand_numbers_code) {
            configurationerror($configfile,"expand_numbers coderef required",getlogger(__PACKAGE__));
        }
        
        $create_lnp_multithreading = $data->{create_lnp_multithreading} if exists $data->{create_lnp_multithreading};
        $create_lnp_numofthreads = _get_numofthreads($create_lnp_numofthreads,$data,'create_lnp_numofthreads');
        
        $delete_lnp_multithreading = $data->{delete_lnp_multithreading} if exists $data->{delete_lnp_multithreading};
        $delete_lnp_numofthreads = _get_numofthreads($delete_lnp_numofthreads,$data,'delete_lnp_numofthreads');
        
        $ignore_lnp_numbers_unique = $data->{ignore_lnp_numbers_unique} if exists $data->{ignore_lnp_numbers_unique};
        $lnp_numbers_single_row_txn = $data->{lnp_numbers_single_row_txn} if exists $data->{lnp_numbers_single_row_txn};
        
        $lnp_numbers_batch_delete = $data->{lnp_numbers_batch_delete} if exists $data->{lnp_numbers_batch_delete};
        
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

    return $result;

}

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $numofthreads = $default_value;
    $numofthreads = $data->{$key} if exists $data->{$key};
    $numofthreads = $cpucount if $numofthreads > $cpucount;
    return $numofthreads;
}

sub _get_sqlite_db_file {
    my ($run,$name) = @_;
    return ((defined $run and length($run) > 0) ? $run . '_' : '') . $name;
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
