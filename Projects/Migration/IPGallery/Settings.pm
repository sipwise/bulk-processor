package Projects::Migration::IPGallery::Settings;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Globals qw(
    update_working_path
    $input_path
    $enablemultithreading
    $cpucount
);

use Logging qw(
    getlogger
    scriptinfo
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    $defaultsettings
    $defaultconfig
    $features_define_filename
    $feature_define_import_numofthreads
    $skip_duplicate_setoptionitems

    $import_multithreading
    $run_id
    $dry
    $force
    $import_db_file
    check_dry
);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $features_define_filename = undef;

our $import_multithreading = $enablemultithreading;
our $feature_define_import_numofthreads = $cpucount;

our $skip_duplicate_setoptionitems = 0;

our $force = 0;
our $dry = 1;
our $run_id = '';

our $import_db_file = ((defined $run_id and length($run_id) > 0) ? '_' : '') . 'import';

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

        #print "$configlogger narf";
        #&$configurationinfocode("testinfomessage",$configlogger);

        $features_define_filename = $data->{features_define_filename} if exists $data->{features_define_filename};
        if (defined $features_define_filename and length($features_define_filename) > 0) {
            $features_define_filename = $input_path . $features_define_filename unless -e $features_define_filename;
        }

        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};
        #my $new_working_path = (exists $data->{working_path} ? $data->{working_path} : $working_path);

        $feature_define_import_numofthreads = $cpucount;
$feature_define_import_numofthreads = $data->{feature_define_import_numofthreads} if exists $data->{feature_define_import_numofthreads};
        $feature_define_import_numofthreads = $cpucount if $feature_define_import_numofthreads > $cpucount;
        #return update_working_path($new_working_path,1,$fileerrorcode,$configlogger);

        $import_db_file = ((defined $run_id and length($run_id) > 0) ? '_' : '') . 'import';

        $dry = $data->{dry} if exists $data->{dry};

        return 1;

    }
    return 0;

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
