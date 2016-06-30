use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Getopt::Long;

use Globals qw(
    $defaultconfig
);
use Projects::Migration::IPGallery::Settings qw(
    $defaultsettings
    update_settings
);
use Logging qw(
    init_log
    getlogger
    $attachmentlogfile
);
use LogError qw (
    completion
    success
);
use LoadConfig qw(
    load_config
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
);
use Utils qw(getscriptpath);
use Mail qw(wrap_mailbody
		    $signature
		    $normalpriority
		    $lowpriority
		    $highpriority);

#use ConnectorPool qw();

use Projects::Migration::IPGallery::Import qw(
    import_features_define
);

my @MODES = ();

if (init() && main()) {
    exit(0);
} else {
    exit(1);
}

sub init {

    #GetOptions ("host=s" => \$host,
    #            "port=i" => \$port,
    #            "file=s" => \$output_filename,
    #            "dir=s"  => \$output_dir,
    #            "user=s" => \$user,
    #            "pass=s" => \$pass,
    #            "period=s" => \$period,
    #            'verbose+' => \$verbose) or fatal("Error in command line arguments");
    my $configfile = $defaultconfig;
    my $settingsfile = $defaultsettings;

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    #update_working_path('/var/sipwise');
#my $logger = getlogger(getscriptpath());
    return $result;

}


sub main() {

    my @messages = ();
    my @attachmentfiles = ();
    my $result = 0;
    my $completion = 0;

    if (1 or ('xx' eq "mode")) { #$mode) {
        $result = import_features_define_task(\@messages);
        $completion = 1;
    } else {
        push(@messages,'unknow option yy, must be one of' . @MODES);
    }

    push(@attachmentfiles,$attachmentlogfile);
    if ($completion) {
        completion(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
    } else {
        success(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
    }

    #ConnectorPool::destroy_dbs();

    return $result;
}

sub cleanup_task {

}

sub import_features_define_task {

    my ($messages) = shift;
    if (import_features_define(
            '/home/rkrenn/test/Features_Define.cfg'
        )) {
        push(@$messages,'sucessfully inserted x records...');
        return 1;
    } else {
        push(@$messages,'some error happened');
        return 0;
    }

}
