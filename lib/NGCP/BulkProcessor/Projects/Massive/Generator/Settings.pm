package NGCP::BulkProcessor::Projects::Massive::Generator::Settings;
use strict;

## no critic

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
    filewarn
    configurationwarn
    configurationerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    split_tuple
    parse_regexp
);
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits);
#format_number check_ipnet

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    check_dry

    $input_path
    $output_path

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $provision_subscriber_count
    $webpassword_length
    $webusername_length
    $sippassword_length
    $sipusername_length

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

our $provision_subscriber_multithreading = $enablemultithreading;
our $provision_subscriber_numofthreads = $cpucount;
our $webpassword_length = 8;
our $webusername_length = 8;
our $sippassword_length = 16;
our $sipusername_length = 8;
our $provision_subscriber_count = 0;


sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);
        #if ($data->{report_filename}) {
        #    $report_filename = $output_path . sprintf('/' . $data->{report_filename},timestampdigits());
        #    if (-e $report_filename and (unlink $report_filename) == 0) {
        #        filewarn('cannot remove ' . $report_filename . ': ' . $!,getlogger(__PACKAGE__));
        #        $report_filename = undef;
        #    }
        #} else {
        #    $report_filename = undef;
        #}

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $provision_subscriber_multithreading = $data->{provision_subscriber_multithreading} if exists $data->{provision_subscriber_multithreading};
        $provision_subscriber_numofthreads = _get_numofthreads($cpucount,$data,'provision_subscriber_numofthreads');
        $webpassword_length = $data->{webpassword_length} if exists $data->{webpassword_length};
        $webusername_length = $data->{webusername_length} if exists $data->{webusername_length};
        $sippassword_length = $data->{sippassword_length} if exists $data->{sippassword_length};
        $sipusername_length = $data->{sipusername_length} if exists $data->{sipusername_length};
        $provision_subscriber_count = $data->{provision_subscriber_count} if exists $data->{provision_subscriber_count};

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

    return $result;

}

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $numofthreads = $default_value;
    $numofthreads = $data->{$key} if exists $data->{$key};
    $numofthreads = $cpucount if $numofthreads > $cpucount;
    return $numofthreads;
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
