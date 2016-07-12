package NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

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
    configurationwarn
    configurationerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    split_tuple
    parse_regexp
);
use NGCP::BulkProcessor::Utils qw(format_number prompt);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    check_dry

    $input_path
    $output_path
    $rollback_path

    $defaultsettings
    $defaultconfig

    $import_multithreading
    $run_id
    $dry
    $force
    $import_db_file

    $features_define_filename
    $features_define_import_numofthreads
    $skip_duplicate_setoptionitems
    $ignore_options_unique
    $ignore_setoptionitems_unique

    $subscriber_define_filename
    $subscriber_define_import_numofthreads
    $subscribernumer_exclude_pattern
    $subscribernumer_exclude_exception_pattern
    $ignore_subscriber_unique
    $skip_prepaid_subscribers

    $lnp_define_filename
    $lnp_define_import_numofthreads
    $ignore_lnp_unique

    $user_password_filename
    $user_password_import_numofthreads
    $ignore_user_password_unique

    $batch_filename
    $batch_import_numofthreads
    $ignore_batch_unique

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';
our $rollback_path = $working_path . 'rollback/';

our $force = 0;
our $dry = 0;
our $run_id = '';
our $import_db_file = _get_import_db_file($run_id,'import');
our $import_multithreading = $enablemultithreading;

our $features_define_filename = undef;
our $features_define_import_numofthreads = $cpucount;
our $skip_duplicate_setoptionitems = 1;
our $ignore_options_unique = 0;
our $ignore_setoptionitems_unique = 0;

our $subscriber_define_filename = undef;
our $subscriber_define_import_numofthreads = $cpucount;
our $subscribernumer_exclude_pattern = undef;
our $subscribernumer_exclude_exception_pattern = undef;
our $ignore_subscriber_unique = 0;
our $skip_prepaid_subscribers = 1;

our $lnp_define_filename = undef;
our $lnp_define_import_numofthreads = $cpucount;
our $ignore_lnp_unique = 1;

our $user_password_filename = undef;
our $user_password_import_numofthreads = $cpucount;
our $ignore_user_password_unique = 0;

our $batch_filename = undef;
our $batch_import_numofthreads = $cpucount;
our $ignore_batch_unique = 0;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $dry = $data->{dry} if exists $data->{dry};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};

        $features_define_filename = _get_import_filename($features_define_filename,$data,'features_define_filename');
        $features_define_import_numofthreads =_get_import_numofthreads($cpucount,$data,'features_define_import_numofthreads');

        $subscriber_define_filename = _get_import_filename($subscriber_define_filename,$data,'subscriber_define_filename');
        $subscriber_define_import_numofthreads = _get_import_numofthreads($cpucount,$data,'subscriber_define_import_numofthreads');

        $subscribernumer_exclude_pattern = $data->{subscribernumer_exclude_pattern} if exists $data->{subscribernumer_exclude_pattern};
        (my $regexp_result,$subscribernumer_exclude_pattern) = parse_regexp($subscribernumer_exclude_pattern,$configfile);
        $result &= $regexp_result;
        $subscribernumer_exclude_exception_pattern = $data->{subscribernumer_exclude_exception_pattern} if exists $data->{subscribernumer_exclude_exception_pattern};
        (my $regexp_result,$subscribernumer_exclude_exception_pattern) = parse_regexp($subscribernumer_exclude_exception_pattern,$configfile);
        $result &= $regexp_result;

        $lnp_define_filename = _get_import_filename($lnp_define_filename,$data,'lnp_define_filename');
        $lnp_define_import_numofthreads = _get_import_numofthreads($cpucount,$data,'lnp_define_import_numofthreads');

        $user_password_filename = _get_import_filename($user_password_filename,$data,'user_password_filename');
        $user_password_import_numofthreads = _get_import_numofthreads($cpucount,$data,'user_password_import_numofthreads');

        $batch_filename = _get_import_filename($batch_filename,$data,'batch_filename');
        $batch_import_numofthreads = _get_import_numofthreads($cpucount,$data,'batch_import_numofthreads');


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
    ($path_result,$rollback_path) = create_path($working_path . 'rollback',$rollback_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

sub _get_import_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $import_numofthreads = $default_value;
    $import_numofthreads = $data->{$key} if exists $data->{$key};
    $import_numofthreads = $cpucount if $import_numofthreads > $cpucount;
    return $import_numofthreads;
}

sub _get_import_db_file {
    my ($run,$name) = @_;
    return ((defined $run and length($run) > 0) ? '_' : '') . $name;
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
