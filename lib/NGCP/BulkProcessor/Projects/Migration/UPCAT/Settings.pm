package NGCP::BulkProcessor::Projects::Migration::UPCAT::Settings;
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
    update_cc_ac_map
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

    $provision_subscriber_rownum_start
    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $webpassword_length
    $webusername_length
    $sippassword_length

    $default_domain
    $default_reseller_name
    $default_billing_profile_name
    $default_barring

    $cc_ac_map_yml
    $cc_ac_map
    $default_cc
    $cc_len_min
    $cc_len_max
    $ac_len

    $barring_profiles_yml
    $barring_profiles
);
#$default_channels_map

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
our $import_multithreading = 0; #$enablemultithreading;

our @subscriber_filenames = ();
our $subscriber_import_numofthreads = $cpucount;
our $ignore_subscriber_unique = 0;
our $subscriber_import_single_row_txn = 1;

our $provision_subscriber_rownum_start = 0; #all lines
our $provision_subscriber_multithreading = $enablemultithreading;
our $provision_subscriber_numofthreads = $cpucount;
our $webpassword_length = 8;
our $webusername_length = 8;
our $sippassword_length = 16;

our $default_domain = undef;
our $default_reseller_name = 'default';
our $default_billing_profile_name = 'Default Billing Profile';
our $default_barring = undef;

our $cc_ac_map_yml = 'cc_ac.yml';
our $cc_ac_map = {};
our $default_cc = undef;
our $cc_len_min = ~0;
our $cc_len_max = 0;
our $ac_len = {};

our $barring_profiles_yml = undef;
our $barring_profiles = {};

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
                $report_filename = undef;
            }
        } else {
            $report_filename = undef;
        }

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};
        #if ($import_multithreading) {
        #    configurationerror($configfile,"import_multithreading must be disabled to preserve record order",getlogger(__PACKAGE__));
        #}

        @subscriber_filenames = _get_import_filenames(\@subscriber_filenames,$data,'subscriber_filenames');
        $subscriber_import_numofthreads = _get_numofthreads($cpucount,$data,'subscriber_import_numofthreads');
        $ignore_subscriber_unique = $data->{ignore_subscriber_unique} if exists $data->{ignore_subscriber_unique};
        $subscriber_import_single_row_txn = $data->{subscriber_import_single_row_txn} if exists $data->{subscriber_import_single_row_txn};

        $provision_subscriber_rownum_start = $data->{provision_subscriber_rownum_start} if exists $data->{provision_subscriber_rownum_start};
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
        $sippassword_length = $data->{sippassword_length} if exists $data->{sippassword_length};
        if (not defined $sippassword_length or $sippassword_length <= 7) {
            configurationerror($configfile,'sippassword_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        #$default_channels = $data->{default_channels} if exists $data->{default_channels};

        $default_domain = $data->{default_domain} if exists $data->{default_domain};
        $default_reseller_name = $data->{default_reseller_name} if exists $data->{default_reseller_name};
        $default_billing_profile_name = $data->{default_billing_profile_name} if exists $data->{default_billing_profile_name};
        $default_barring = $data->{default_barring} if exists $data->{default_barring};
        $cc_ac_map_yml = $data->{cc_ac_map_yml} if exists $data->{cc_ac_map_yml};
        $default_cc = $data->{default_cc} if exists $data->{default_cc};

        $barring_profiles_yml = $data->{barring_profiles_yml} if exists $data->{barring_profiles_yml};

        return $result;

    }
    return 0;

}

sub update_cc_ac_map {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $cc_ac_map = $data;
        };
        if ($@ or 'HASH' ne ref $cc_ac_map) {
            $cc_ac_map //= {};
            configurationerror($configfile,'invalid cc ac map',getlogger(__PACKAGE__));
            $result = 0;
        } else {
            foreach my $cc (keys %$cc_ac_map) {
                my $ac_map = $cc_ac_map->{$cc};
                $cc_len_min = length($cc) if length($cc) < $cc_len_min;
                $cc_len_max = length($cc) if length($cc) > $cc_len_max;
                $ac_len->{$cc} = { min => ~0, max => 0, };
                if ('HASH' ne ref $ac_map) {
                    configurationerror($configfile,"invalid $cc ac map",getlogger(__PACKAGE__));
                    $result = 0;
                } else {
                    foreach my $ac (keys %$ac_map) {
                        if ($ac_map->{$ac}) { # ac enabled
                            $ac_len->{$cc}->{min} = length($ac) if length($ac) < $ac_len->{$cc}->{min};
                            $ac_len->{$cc}->{max} = length($ac) if length($ac) > $ac_len->{$cc}->{max};
                        } else {
                            delete $ac_map->{$ac};
                        }
                    }
                }
            }
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
            $barring_profiles = $data; #->{'mapping'};
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

1;
