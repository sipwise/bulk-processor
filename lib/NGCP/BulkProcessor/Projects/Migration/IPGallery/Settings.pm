package NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use NGCP::BulkProcessor::Globals qw(
    update_working_path
    $input_path
    $enablemultithreading
    $cpucount
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    scriptinfo
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    check_dry
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

    $subscriber_define_filename
    $subscriber_define_import_numofthreads

    $lnp_define_filename
    $lnp_define_import_numofthreads

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $force = 0;
our $dry = 1;
our $run_id = '';
our $import_db_file = _get_import_db_file($run_id,'import');
our $import_multithreading = $enablemultithreading;

our $features_define_filename = undef;
our $features_define_import_numofthreads = $cpucount;
our $skip_duplicate_setoptionitems = 0;

our $subscriber_define_filename = undef;
our $subscriber_define_import_numofthreads = $cpucount;

our $lnp_define_filename = undef;
our $lnp_define_import_numofthreads = $cpucount;

sub update_settings {

    my ($data,$configfile,
        $split_tuplecode,
        $format_number,
        $configurationinfocode,
        $configurationwarncode,
        $configurationerrorcode,
        $fileerrorcode,
        $configlogger) = @_;

    if (defined $data) {

        #&$configurationinfocode("testinfomessage",$configlogger);
        $dry = $data->{dry} if exists $data->{dry};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};

        $features_define_filename = _get_import_filename($features_define_filename,$data,'features_define_filename');
        $features_define_import_numofthreads =_get_import_numofthreads($cpucount,$data,'features_define_import_numofthreads');

        $subscriber_define_filename = _get_import_filename($subscriber_define_filename,$data,'subscriber_define_filename');
        $subscriber_define_import_numofthreads = _get_import_numofthreads($cpucount,$data,'subscriber_define_import_numofthreads');

        $lnp_define_filename = _get_import_filename($lnp_define_filename,$data,'lnp_define_filename');
        $lnp_define_import_numofthreads= _get_import_numofthreads($cpucount,$data,'lnp_define_import_numofthreads');

        return 1;

    }
    return 0;

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
