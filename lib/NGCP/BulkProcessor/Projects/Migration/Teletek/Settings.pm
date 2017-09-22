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
    update_reseller_mapping
    update_barring_profiles
    check_dry

    $input_path
    $output_path
    $report_filename

    $defaultsettings
    $defaultconfig

    $import_multithreading
    $run_id
    $dry
    $skip_errors
    $force
    $import_db_file

    @subscriber_filenames
    $subscriber_import_numofthreads
    $ignore_subscriber_unique
    $subscriber_import_single_row_txn
    $subscriber_import_unfold_ranges
    $reseller_mapping_yml
    $reseller_mapping
    $barring_profiles_yml
    $barring_profiles

    @allowedcli_filenames
    $allowedcli_import_numofthreads
    $ignore_allowedcli_unique
    $allowedcli_import_single_row_txn
    $allowedcli_import_unfold_ranges

    @clir_filenames
    $clir_import_numofthreads
    $ignore_clir_unique
    $clir_import_single_row_txn

    @callforward_filenames
    $callforward_import_numofthreads
    $ignore_callforward_unique
    $callforward_import_single_row_txn

    @registration_filenames
    $registration_import_numofthreads
    $ignore_registration_unique
    $registration_import_single_row_txn

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $webpassword_length
    $webusername_length

    $cf_default_priority
    $cf_default_timeout
    $cft_default_ringtimeout

);
#$concurrent_max_total
#    $set_allowed_ips_multithreading
#    $set_allowed_ips_numofthreads
#    $allowed_ips

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';
our $report_filename = undef;

our $force = 0;
our $dry = 0;
our $skip_errors = 0;
our $run_id = '';
our $import_db_file = _get_import_db_file($run_id,'import');
our $import_multithreading = $enablemultithreading;

our @subscriber_filenames = ();
our $subscriber_import_numofthreads = $cpucount;
our $ignore_subscriber_unique = 0;
our $subscriber_import_single_row_txn = 1;
our $subscriber_import_unfold_ranges = 1;
our $reseller_mapping_yml = undef;
our $reseller_mapping = {};
our $barring_profiles_yml = undef;
our $barring_profiles = {};

our @allowedcli_filenames = ();
our $allowedcli_import_numofthreads = $cpucount;
our $ignore_allowedcli_unique = 0;
our $allowedcli_import_single_row_txn = 1;
our $allowedcli_import_unfold_ranges = 1;

our @clir_filenames = ();
our $clir_import_numofthreads = $cpucount;
our $ignore_clir_unique = 0;
our $clir_import_single_row_txn = 1;

our @callforward_filenames = ();
our $callforward_import_numofthreads = $cpucount;
our $ignore_callforward_unique = 0;
our $callforward_import_single_row_txn = 1;

our @registration_filenames = ();
our $registration_import_numofthreads = $cpucount;
our $ignore_registration_unique = 0;
our $registration_import_single_row_txn = 1;

our $provision_subscriber_multithreading = $enablemultithreading;
our $provision_subscriber_numofthreads = $cpucount;
our $webpassword_length = 8;
our $webusername_length = 8;


our $cf_default_priority = 1;
our $cf_default_timeout = 300;
our $cft_default_ringtimeout = 20;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);
        if ($data->{report_filename}) {
            $report_filename = $output_path . sprintf('/' . $data->{report_filename},timestampdigits());
            if (-e $report_filename and (unlink $report_filename) == 0) {
                filewarn('cannot remove ' . $report_filename . ': ' . $!,getlogger(__PACKAGE__));
            }
        } else {
            $report_filename = undef;
        }

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};

        @subscriber_filenames = _get_import_filenames(\@subscriber_filenames,$data,'subscriber_filenames');
        $subscriber_import_numofthreads = _get_numofthreads($cpucount,$data,'subscriber_import_numofthreads');
        $ignore_subscriber_unique = $data->{ignore_subscriber_unique} if exists $data->{ignore_subscriber_unique};
        $subscriber_import_single_row_txn = $data->{subscriber_import_single_row_txn} if exists $data->{subscriber_import_single_row_txn};
        $subscriber_import_unfold_ranges = $data->{subscriber_import_unfold_ranges} if exists $data->{subscriber_import_unfold_ranges};
        $reseller_mapping_yml = $data->{reseller_mapping_yml} if exists $data->{reseller_mapping_yml};
        $barring_profiles_yml = $data->{barring_profiles_yml} if exists $data->{barring_profiles_yml};

        @allowedcli_filenames = _get_import_filenames(\@allowedcli_filenames,$data,'allowedcli_filenames');
        $allowedcli_import_numofthreads = _get_numofthreads($cpucount,$data,'allowedcli_import_numofthreads');
        $ignore_allowedcli_unique = $data->{ignore_allowedcli_unique} if exists $data->{ignore_allowedcli_unique};
        $allowedcli_import_single_row_txn = $data->{allowedcli_import_single_row_txn} if exists $data->{allowedcli_import_single_row_txn};
        $allowedcli_import_unfold_ranges = $data->{allowedcli_import_unfold_ranges} if exists $data->{allowedcli_import_unfold_ranges};

        @clir_filenames = _get_import_filenames(\@clir_filenames,$data,'clir_filenames');
        $clir_import_numofthreads = _get_numofthreads($cpucount,$data,'clir_import_numofthreads');
        $ignore_clir_unique = $data->{ignore_clir_unique} if exists $data->{ignore_clir_unique};
        $clir_import_single_row_txn = $data->{clir_import_single_row_txn} if exists $data->{clir_import_single_row_txn};

        @callforward_filenames = _get_import_filenames(\@callforward_filenames,$data,'callforward_filenames');
        $callforward_import_numofthreads = _get_numofthreads($cpucount,$data,'callforward_import_numofthreads');
        $ignore_callforward_unique = $data->{ignore_callforward_unique} if exists $data->{ignore_callforward_unique};
        $callforward_import_single_row_txn = $data->{callforward_import_single_row_txn} if exists $data->{callforward_import_single_row_txn};

        @registration_filenames = _get_import_filenames(\@registration_filenames,$data,'registration_filenames');
        $registration_import_numofthreads = _get_numofthreads($cpucount,$data,'registration_import_numofthreads');
        $ignore_registration_unique = $data->{ignore_registration_unique} if exists $data->{ignore_registration_unique};
        $registration_import_single_row_txn = $data->{registration_import_single_row_txn} if exists $data->{registration_import_single_row_txn};

        $provision_subscriber_multithreading = $data->{provision_subscriber_multithreading} if exists $data->{provision_subscriber_multithreading};
        $provision_subscriber_numofthreads = _get_numofthreads($cpucount,$data,'provision_subscriber_numofthreads');
        $webpassword_length = $data->{webpassword_length} if exists $data->{webpassword_length};
        if (not defined $webpassword_length or $webpassword_length <= 7) {
            configurationerror($configfile,'webpassword_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        $webusername_length = $data->{webusername_length} if exists $data->{webusername_length};
        if (not defined $webusername_length or $webusername_length <= 7) {
            configurationerror($configfile,'webusername_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }

        $cf_default_priority = $data->{cf_default_priority} if exists $data->{cf_default_priority};
        $cf_default_timeout = $data->{cf_default_timeout} if exists $data->{cf_default_timeout};
        $cft_default_ringtimeout = $data->{cft_default_ringtimeout} if exists $data->{cft_default_ringtimeout};

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

sub _get_import_db_file {
    my ($run,$name) = @_;
    return ((defined $run and length($run) > 0) ? $run . '_' : '') . $name;
}

sub _get_import_filenames {
    my ($old_value,$data,$key) = @_;
    my @import_filenames = @$old_value;
    @import_filenames = split_tuple($data->{$key}) if exists $data->{$key};
    my @result = ();
    foreach my $import_filename (@import_filenames) {
        if (defined $import_filename and length($import_filename) > 0) {
            $import_filename = $input_path . $import_filename unless -e $import_filename;
            push(@result,$import_filename);
        }
    }
    return @result;
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

sub update_reseller_mapping {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $reseller_mapping = $data->{'mapping'};
        };
        if ($@ or 'HASH' ne ref $reseller_mapping) { # or (scalar keys %$reseller_mapping) == 0) {
            $reseller_mapping //= {};
            configurationerror($configfile,'invalid reseller mapping',getlogger(__PACKAGE__));
            $result = 0;
        }

        return $result;
    }
    return 0;

}

sub update_barring_profiles {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $barring_profiles = $data->{'mapping'};
        };
        if ($@ or 'HASH' ne ref $barring_profiles or (scalar keys %$barring_profiles) == 0) {
            $barring_profiles //= {};
            configurationerror($configfile,'no barring mappings found',getlogger(__PACKAGE__));
            $result = 0;
        }

        return $result;
    }
    return 0;

}

1;
