package Projects::Migration::IPGallery::Settings;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    $defaultsettings
);

our $defaultsettings = 'settings.cfg';






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

print "$configlogger narf";
        &$configurationinfocode("testinfomessage",$configlogger);
        # databases - dsp
        #$accounting_host = $config->{accounting_host} if exists $config->{accounting_host};

        return 1;
        
    }
    return 0;
    
}


1;