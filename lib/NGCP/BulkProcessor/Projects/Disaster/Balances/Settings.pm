package NGCP::BulkProcessor::Projects::Disaster::Balances::Settings;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
    $cpucount
);
#$working_path
#create_path

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
use NGCP::BulkProcessor::Utils qw(prompt);
#format_number check_ipnet

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    check_dry

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        #my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        #$result &= _prepare_working_paths(1);

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};


        return $result;

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

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $import_numofthreads = $default_value;
    $import_numofthreads = $data->{$key} if exists $data->{$key};
    $import_numofthreads = $cpucount if $import_numofthreads > $cpucount;
    return $import_numofthreads;
}

1;
