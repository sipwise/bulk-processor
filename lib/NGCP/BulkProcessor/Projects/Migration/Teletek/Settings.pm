package NGCP::BulkProcessor::Projects::Migration::Teletek::Settings;
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
    configurationwarn
    configurationerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    split_tuple
    parse_regexp
);
use NGCP::BulkProcessor::Utils qw(format_number check_ipnet prompt);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    check_dry

    $input_path
    $output_path

    $defaultsettings
    $defaultconfig

    $import_multithreading
    $run_id
    $dry
    $skip_errors
    $force
    $import_db_file

    $subscriber_filename
    $subscriber_import_numofthreads
    $ignore_subscriber_unique
    $subscriber_import_single_row_txn
    $subscriber_import_unfold_ranges

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $always_update_subscriber

    $set_allowed_ips_multithreading
    $set_allowed_ips_numofthreads
    $allowed_ips

    $set_call_forwards_multithreading
    $set_call_forwards_numofthreads
    $cfb_priorities
    $cfb_timeouts
    $cfu_priorities
    $cfu_timeouts
    $cft_priorities
    $cft_timeouts
    $cfna_priorities
    $cfna_timeouts
    $cfnumber_exclude_pattern
    $cfnumber_trim_pattern
    $ringtimeout

    $set_preference_bulk_multithreading
    $set_preference_bulk_numofthreads

);
#$concurrent_max_total

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;
our $run_id = '';
our $import_db_file = _get_import_db_file($run_id,'import');
our $import_multithreading = $enablemultithreading;

our $subscriber_filename = undef;
our $subscriber_import_numofthreads = $cpucount;
our $ignore_subscriber_unique = 0;
our $subscriber_import_single_row_txn = 1;
our $subscriber_import_unfold_ranges = 1;

our $provision_subscriber_multithreading = $enablemultithreading;
our $provision_subscriber_numofthreads = $cpucount;
our $always_update_subscriber = 0;

our $set_allowed_ips_multithreading = $enablemultithreading;
our $set_allowed_ips_numofthreads = $cpucount;
our $allowed_ips = [];

our $set_call_forwards_multithreading = $enablemultithreading;
our $set_call_forwards_numofthreads = $cpucount;
our $cfb_priorities = [];
our $cfb_timeouts = [];
our $cfu_priorities = [];
our $cfu_timeouts = [];
our $cft_priorities = [];
our $cft_timeouts = [];
our $cfna_priorities = [];
our $cfna_timeouts = [];
our $cfnumber_exclude_pattern = undef;
our $cfnumber_trim_pattern = undef;
our $ringtimeout = undef;

our $set_preference_bulk_multithreading = $enablemultithreading;
our $set_preference_bulk_numofthreads = $cpucount;
our $concurrent_max_total = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};

        $subscriber_filename = _get_import_filename($subscriber_filename,$data,'subscriber_filename');
        $subscriber_import_numofthreads = _get_numofthreads($cpucount,$data,'subscriber_import_numofthreads');
        $ignore_subscriber_unique = $data->{ignore_subscriber_unique} if exists $data->{ignore_subscriber_unique};
        $subscriber_import_single_row_txn = $data->{subscriber_import_single_row_txn} if exists $data->{subscriber_import_single_row_txn};
        $subscriber_import_unfold_ranges = $data->{subscriber_import_unfold_ranges} if exists $data->{subscriber_import_unfold_ranges};

        $provision_subscriber_multithreading = $data->{provision_subscriber_multithreading} if exists $data->{provision_subscriber_multithreading};
        $provision_subscriber_numofthreads = _get_numofthreads($cpucount,$data,'provision_subscriber_numofthreads');
        $always_update_subscriber = $data->{always_update_subscriber} if exists $data->{always_update_subscriber};

        $set_allowed_ips_multithreading = $data->{set_allowed_ips_multithreading} if exists $data->{set_allowed_ips_multithreading};
        $set_allowed_ips_numofthreads = _get_numofthreads($cpucount,$data,'set_allowed_ips_numofthreads');
        $allowed_ips = [ split_tuple($data->{allowed_ips}) ] if exists $data->{allowed_ips};
        foreach my $ipnet (@$allowed_ips) {
            if (not check_ipnet($ipnet)) {
                configurationerror($configfile,"invalid allowed_ip '$ipnet'",getlogger(__PACKAGE__));
                $result = 0;
            }
        }

        $set_call_forwards_multithreading = $data->{set_call_forwards_multithreading} if exists $data->{set_call_forwards_multithreading};
        $set_call_forwards_numofthreads = _get_numofthreads($cpucount,$data,'set_call_forwards_numofthreads');
        $cfb_priorities = [ split_tuple($data->{cfb_priorities}) ] if exists $data->{cfb_priorities};
        $cfb_timeouts = [ split_tuple($data->{cfb_timeouts}) ] if exists $data->{cfb_timeouts};
        $cfu_priorities = [ split_tuple($data->{cfu_priorities}) ] if exists $data->{cfu_priorities};
        $cfu_timeouts = [ split_tuple($data->{cfu_timeouts}) ] if exists $data->{cfu_timeouts};
        $cft_priorities = [ split_tuple($data->{cft_priorities}) ] if exists $data->{cft_priorities};
        $cft_timeouts = [ split_tuple($data->{cft_timeouts}) ] if exists $data->{cft_timeouts};
        $cfna_priorities = [ split_tuple($data->{cfna_priorities}) ] if exists $data->{cfna_priorities};
        $cfna_timeouts = [ split_tuple($data->{cfna_timeouts}) ] if exists $data->{cfna_timeouts};
        $cfnumber_exclude_pattern = $data->{cfnumber_exclude_pattern} if exists $data->{cfnumber_exclude_pattern};
        ($regexp_result,$cfnumber_exclude_pattern) = parse_regexp($cfnumber_exclude_pattern,$configfile);
        $result &= $regexp_result;
        $cfnumber_trim_pattern = $data->{cfnumber_trim_pattern} if exists $data->{cfnumber_trim_pattern};
        ($regexp_result,$cfnumber_trim_pattern) = parse_regexp($cfnumber_trim_pattern,$configfile);
        $result &= $regexp_result;
        $ringtimeout = $data->{ringtimeout} if exists $data->{ringtimeout};
        if (not defined $ringtimeout or $ringtimeout <= 0) {
            configurationerror($configfile,'ringtimeout greater than 0 required',getlogger(__PACKAGE__));
            $result = 0;
        }

        $set_preference_bulk_multithreading = $data->{set_preference_bulk_multithreading} if exists $data->{set_preference_bulk_multithreading};
        $set_preference_bulk_numofthreads = _get_numofthreads($cpucount,$data,'set_preference_bulk_numofthreads');
        #$concurrent_max_total = $data->{concurrent_max_total} if exists $data->{concurrent_max_total};
        #if (defined $concurrent_max_total and $concurrent_max_total <= 0) {
        #    configurationerror($configfile,'empty concurrent_max_total or greater than 0 required',getlogger(__PACKAGE__));
        #    $result = 0;
        #}

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
    my $_numofthreads = $default_value;
    $_numofthreads = $data->{$key} if exists $data->{$key};
    $_numofthreads = $cpucount if $_numofthreads > $cpucount;
    return $_numofthreads;
}

sub _get_import_db_file {
    my ($run,$name) = @_;
    return ((defined $run and length($run) > 0) ? $run . '_' : '') . $name;
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
