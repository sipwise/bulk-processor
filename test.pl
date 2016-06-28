use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use Getopt::Long;

use Globals qw($defaultconfig);
use Logging qw(
    init_log
    getlogger
    $attachmentlogfile
);
use LogError qw (
    completion
    success
);
use LoadConfig qw(load_config);
use Utils qw(getscriptpath zerofill changemod timestampdigits);
use Mail qw(wrap_mailbody
		    $signature
		    $normalpriority
		    $lowpriority
		    $highpriority);
use ConnectorPool qw(destroy_dbs);

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

    load_config($configfile);
    init_log();
#my $logger = getlogger(getscriptpath());
    return 0; #blah;

}


sub main() {
    
    return 0;
}