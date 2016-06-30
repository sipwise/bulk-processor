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
use Utils qw(getscriptpath zerofill changemod timestampdigits);
use Mail qw(wrap_mailbody
		    $signature
		    $normalpriority
		    $lowpriority
		    $highpriority);
use ConnectorPool qw(destroy_dbs);

use FileProcessors::CSVFile;

init();

exit(main());

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

    my $importer = FileProcessors::CSVFile->new();

    $importer->process('/home/rkrenn/test/test.csv',sub {
        my ($rowblock,$i) = @_;
        print "!!!!!!!!!!!!!!!!!!!!!!" . (scalar @$rowblock) . " rows read!\n";
        return 1;
    });

    return 0;
}
