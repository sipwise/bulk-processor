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
    split_number
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

    @mta_subscriber_filenames
    $mta_subscriber_import_numofthreads
    $ignore_mta_subscriber_unique
    $mta_subscriber_import_single_row_txn

    $provision_mta_subscriber_rownum_start
    $provision_mta_subscriber_multithreading
    $provision_mta_subscriber_numofthreads
    $mta_webpassword_length
    $mta_webusername_length
    $mta_sippassword_length

    $mta_default_domain
    $mta_default_reseller_name
    $mta_default_billing_profile_name
    $mta_default_barring

    $cc_ac_map_yml
    $cc_ac_map
    $default_cc
    $cc_len_min
    $cc_len_max
    $ac_len

    $barring_profiles_yml
    $barring_profiles

    @ccs_subscriber_filenames
    $ignore_ccs_subscriber_unique
    $provision_ccs_subscriber_rownum_start
    $ccs_subscriber_import_single_row_txn
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

our @mta_subscriber_filenames = ();
our $mta_subscriber_import_numofthreads = $cpucount;
our $ignore_mta_subscriber_unique = 0;
our $mta_subscriber_import_single_row_txn = 1;

our $provision_mta_subscriber_rownum_start = 0; #all lines
our $provision_mta_subscriber_multithreading = $enablemultithreading;
our $provision_mta_subscriber_numofthreads = $cpucount;
our $mta_webpassword_length = 8;
our $mta_webusername_length = 8;
our $mta_sippassword_length = 16;

our $mta_default_domain = undef;
our $mta_default_reseller_name = 'default';
our $mta_default_billing_profile_name = 'Default Billing Profile';
our $mta_default_barring = undef;

our $cc_ac_map_yml = 'cc_ac.yml';
our $cc_ac_map = {};
our $default_cc = undef;
our $cc_len_min = ~0;
our $cc_len_max = 0;
our $ac_len = {};

our $barring_profiles_yml = undef;
our $barring_profiles = {};

our @ccs_subscriber_filenames = ();
our $ignore_ccs_subscriber_unique = 0;
our $provision_ccs_subscriber_rownum_start = 0;
our $ccs_subscriber_import_single_row_txn = 1;

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

        @mta_subscriber_filenames = _get_import_filenames(\@mta_subscriber_filenames,$data,'mta_subscriber_filenames');
        $mta_subscriber_import_numofthreads = _get_numofthreads($cpucount,$data,'mta_subscriber_import_numofthreads');
        $ignore_mta_subscriber_unique = $data->{ignore_mta_subscriber_unique} if exists $data->{ignore_mta_subscriber_unique};
        $mta_subscriber_import_single_row_txn = $data->{mta_subscriber_import_single_row_txn} if exists $data->{mta_subscriber_import_single_row_txn};

        $provision_mta_subscriber_rownum_start = $data->{provision_mta_subscriber_rownum_start} if exists $data->{provision_mta_subscriber_rownum_start};
        $provision_mta_subscriber_multithreading = $data->{provision_mta_subscriber_multithreading} if exists $data->{provision_mta_subscriber_multithreading};
        $provision_mta_subscriber_numofthreads = _get_numofthreads($cpucount,$data,'provision_mta_subscriber_numofthreads');
        $mta_webpassword_length = $data->{mta_webpassword_length} if exists $data->{mta_webpassword_length};
        if (not defined $mta_webpassword_length or $mta_webpassword_length <= 7) {
            configurationerror($configfile,'mta_webpassword_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        $mta_webusername_length = $data->{mta_webusername_length} if exists $data->{mta_webusername_length};
        if (not defined $mta_webusername_length or $mta_webusername_length <= 7) {
            configurationerror($configfile,'mta_webusername_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        $mta_sippassword_length = $data->{mta_sippassword_length} if exists $data->{mta_sippassword_length};
        if (not defined $mta_sippassword_length or $mta_sippassword_length <= 7) {
            configurationerror($configfile,'mta_sippassword_length greater than 7 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        #$default_channels = $data->{default_channels} if exists $data->{default_channels};

        $mta_default_domain = $data->{mta_default_domain} if exists $data->{mta_default_domain};
        $mta_default_reseller_name = $data->{mta_default_reseller_name} if exists $data->{mta_default_reseller_name};
        $mta_default_billing_profile_name = $data->{mta_default_billing_profile_name} if exists $data->{mta_default_billing_profile_name};
        $mta_default_barring = $data->{mta_default_barring} if exists $data->{mta_default_barring};

        $cc_ac_map_yml = $data->{cc_ac_map_yml} if exists $data->{cc_ac_map_yml};
        $default_cc = $data->{default_cc} if exists $data->{default_cc};

        $barring_profiles_yml = $data->{barring_profiles_yml} if exists $data->{barring_profiles_yml};

        @ccs_subscriber_filenames = _get_import_filenames(\@ccs_subscriber_filenames,$data,'ccs_subscriber_filenames');
        $ignore_ccs_subscriber_unique = $data->{ignore_ccs_subscriber_unique} if exists $data->{ignore_ccs_subscriber_unique};
        $provision_ccs_subscriber_rownum_start = $data->{provision_ccs_subscriber_rownum_start} if exists $data->{provision_ccs_subscriber_rownum_start};
        $ccs_subscriber_import_single_row_txn = $data->{ccs_subscriber_import_single_row_txn} if exists $data->{ccs_subscriber_import_single_row_txn};

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

sub split_number {
    my ($dn) = @_;
    my ($cc,$ac,$sn) = ('','',$dn);
    if ($cc_ac_map) {
        if ($default_cc) {
            $cc = $default_cc;
            $dn =~ s/^0//;
            $sn = $dn;
        } else {
            foreach my $cc_length ($cc_len_min .. $cc_len_max) {
                my ($_cc,$_dn) = (substr($dn,0,$cc_length), substr($dn,$cc_length));
                if (exists $cc_ac_map->{$_cc}) {
                    $cc = $_cc;
                    $sn = $_dn;
                    $dn = $_dn;
                    last;
                }
            }
        }
        if (exists $cc_ac_map->{$cc}) {
            my $ac_map = $cc_ac_map->{$cc};
            foreach my $ac_length ($ac_len->{$cc}->{min} .. $ac_len->{$cc}->{max}) {
                my ($_ac,$_sn) = (substr($dn,0,$ac_length), substr($dn,$ac_length));
                if (exists $ac_map->{$_ac}) {
                    $ac = $_ac;
                    $sn = $_sn;
                    #$dn = '';
                    last;
                }
            }
        }
    }
    return ($cc,$ac,$sn);

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
