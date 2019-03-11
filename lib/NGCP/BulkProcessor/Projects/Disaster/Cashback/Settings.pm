package NGCP::BulkProcessor::Projects::Disaster::Cashback::Settings;
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
use NGCP::BulkProcessor::Utils qw(prompt stringtobool);
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

    $cashback_multithreading
    $cashback_numofthreads
    $cashback_blocksize

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

our $cashback_multithreading = $enablemultithreading;
our $cashback_numofthreads = $cpucount;
our $cashback_blocksize = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        #my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        #$result &= _prepare_working_paths(1);

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $cashback_multithreading = $data->{cashback_multithreading} if exists $data->{cashback_multithreading};
        $cashback_numofthreads = _get_numofthreads($cpucount,$data,'cashback_numofthreads');
        $cashback_blocksize = $data->{cashback_blocksize} if exists $data->{cashback_blocksize};

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
    my $_numofthreads = $default_value;
    $_numofthreads = $data->{$key} if exists $data->{$key};
    $_numofthreads = $cpucount if $_numofthreads > $cpucount;
    return $_numofthreads;
}

1;
