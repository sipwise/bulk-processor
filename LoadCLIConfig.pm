package LoadCLIConfig;
use strict;

use Getopt::Long;
use Globals qw($defaultconfig);
use LoadConfig qw(load_config);

my $configfile;
my $arg = shift @ARGV;
if (defined $arg) {
    $configfile = $arg;
} else {
    $configfile = $defaultconfig;
}

my $configfile = $defaultconfig;

   
    GetOptions ("host=s" => \$host,
                "port=i" => \$port,
                "file=s" => \$output_filename,
                "dir=s"  => \$output_dir,
                "user=s" => \$user,
                "pass=s" => \$pass,
                "period=s" => \$period,
                'verbose+' => \$verbose) or fatal("Error in command line arguments");

load_config($configfile);

1;