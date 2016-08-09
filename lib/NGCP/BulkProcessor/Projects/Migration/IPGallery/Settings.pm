package NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings;
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
    update_barring_profiles
    check_dry

    $input_path
    $output_path
    $rollback_path

    $defaultsettings
    $defaultconfig

    $import_multithreading
    $run_id
    $dry
    $skip_errors
    $force
    $batch
    $import_db_file

    $features_define_filename
    $features_define_import_numofthreads
    $skip_duplicate_setoptionitems
    $ignore_options_unique
    $ignore_setoptionitems_unique

    $subscriber_define_filename
    $subscriber_define_import_numofthreads
    $subscribernumer_exclude_pattern
    $subscribernumer_exclude_exception_pattern
    $ignore_subscriber_unique
    $skip_prepaid_subscribers

    $lnp_define_filename
    $lnp_define_import_numofthreads
    $ignore_lnp_unique

    $user_password_filename
    $user_password_import_numofthreads
    $ignore_user_password_unique
    $username_prefix
    $min_password_length

    $batch_filename
    $batch_import_numofthreads
    $ignore_batch_unique

    $subscribernumber_pattern

    $reseller_id
    $domain_name
    $subsciber_username_prefix
    $billing_profile_id
    $contact_email_format
    $webpassword_length
    $generate_webpassword

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads

    $set_barring_profiles_multithreading
    $set_barring_profiles_numofthreads
    $barring_profiles_yml
    $barring_profiles

    $set_peer_auth_multithreading
    $set_peer_auth_numofthreads
    $peer_auth_realm

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

    $create_lnps_multithreading
    $create_lnps_numofthreads
    $create_lnp_block_txn

    $set_preference_bulk_multithreading
    $set_preference_bulk_numofthreads
    $concurrent_max_total
);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';
our $output_path = $working_path . 'output/';
our $rollback_path = $working_path . 'rollback/';

our $force = 0;
our $dry = 0;
our $skip_errors = 0;
our $batch = 0;
our $run_id = '';
our $import_db_file = _get_import_db_file($run_id,'import');
our $import_multithreading = $enablemultithreading;

our $features_define_filename = undef;
our $features_define_import_numofthreads = $cpucount;
our $skip_duplicate_setoptionitems = 1;
our $ignore_options_unique = 0;
our $ignore_setoptionitems_unique = 0;

our $subscriber_define_filename = undef;
our $subscriber_define_import_numofthreads = $cpucount;
our $subscribernumer_exclude_pattern = undef;
our $subscribernumer_exclude_exception_pattern = undef;
our $ignore_subscriber_unique = 0;
our $skip_prepaid_subscribers = 1;

our $lnp_define_filename = undef;
our $lnp_define_import_numofthreads = $cpucount;
our $ignore_lnp_unique = 1;

our $user_password_filename = undef;
our $user_password_import_numofthreads = $cpucount;
our $ignore_user_password_unique = 0;
our $username_prefix = undef;
our $min_password_length = 3;

our $batch_filename = undef;
our $batch_import_numofthreads = $cpucount;
our $ignore_batch_unique = 0;

our $subscribernumber_pattern = undef;

our $reseller_id = undef; #1
our $domain_name = undef; #example.org
our $billing_profile_id = undef; #1
our $contact_email_format = undef; #%s@melita.mt
our $webpassword_length = undef;
our $generate_webpassword = 1;
our $subsciber_username_prefix = undef;

our $provision_subscriber_multithreading = $enablemultithreading;
our $provision_subscriber_numofthreads = $cpucount;

our $set_barring_profiles_multithreading = $enablemultithreading;
our $set_barring_profiles_numofthreads = $cpucount;
our $barring_profiles_yml = undef;
our $barring_profiles = {};

our $set_peer_auth_multithreading = $enablemultithreading;
our $set_peer_auth_numofthreads = $cpucount;
our $peer_auth_realm = undef;

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

our $create_lnps_multithreading = $enablemultithreading;
our $create_lnps_numofthreads = $cpucount;
our $create_lnp_block_txn = 0;

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
        $batch = $data->{batch} if exists $data->{batch};
        $import_db_file = _get_import_db_file($run_id,'import');
        $import_multithreading = $data->{import_multithreading} if exists $data->{import_multithreading};

        $features_define_filename = _get_import_filename($features_define_filename,$data,'features_define_filename');
        $features_define_import_numofthreads =_get_import_numofthreads($cpucount,$data,'features_define_import_numofthreads');

        $subscriber_define_filename = _get_import_filename($subscriber_define_filename,$data,'subscriber_define_filename');
        $subscriber_define_import_numofthreads = _get_import_numofthreads($cpucount,$data,'subscriber_define_import_numofthreads');

        $subscribernumer_exclude_pattern = $data->{subscribernumer_exclude_pattern} if exists $data->{subscribernumer_exclude_pattern};
        ($regexp_result,$subscribernumer_exclude_pattern) = parse_regexp($subscribernumer_exclude_pattern,$configfile);
        $result &= $regexp_result;
        $subscribernumer_exclude_exception_pattern = $data->{subscribernumer_exclude_exception_pattern} if exists $data->{subscribernumer_exclude_exception_pattern};
        ($regexp_result,$subscribernumer_exclude_exception_pattern) = parse_regexp($subscribernumer_exclude_exception_pattern,$configfile);
        $result &= $regexp_result;

        $subscribernumber_pattern = $data->{subscribernumber_pattern} if exists $data->{subscribernumber_pattern};
        ($regexp_result,$subscribernumber_pattern) = parse_regexp($subscribernumber_pattern,$configfile);
        $result &= $regexp_result;

        $lnp_define_filename = _get_import_filename($lnp_define_filename,$data,'lnp_define_filename');
        $lnp_define_import_numofthreads = _get_import_numofthreads($cpucount,$data,'lnp_define_import_numofthreads');

        $user_password_filename = _get_import_filename($user_password_filename,$data,'user_password_filename');
        $user_password_import_numofthreads = _get_import_numofthreads($cpucount,$data,'user_password_import_numofthreads');

        $username_prefix = $data->{username_prefix} if exists $data->{username_prefix};
        $min_password_length = $data->{min_password_length} if exists $data->{min_password_length};

        $batch_filename = _get_import_filename($batch_filename,$data,'batch_filename');
        $batch_import_numofthreads = _get_import_numofthreads($cpucount,$data,'batch_import_numofthreads');

        $reseller_id = $data->{reseller_id} if exists $data->{reseller_id};
        $domain_name = $data->{domain_name} if exists $data->{domain_name};
        $billing_profile_id = $data->{billing_profile_id} if exists $data->{billing_profile_id};
        $contact_email_format = $data->{contact_email_format} if exists $data->{contact_email_format};
        if ($contact_email_format !~ /^[a-z0-9.]*%s[a-z0-9.]*\@[a-z0-9.-]+$/gi) {
            configurationerror($configfile,'invalid contact email format',getlogger(__PACKAGE__));
            $result = 0;
        }
        $webpassword_length = $data->{webpassword_length} if exists $data->{webpassword_length};
        if (not defined $webpassword_length or $webpassword_length < 3) {
            configurationerror($configfile,'minimum webpassword length of 3 required',getlogger(__PACKAGE__));
            $result = 0;
        }
        $generate_webpassword = $data->{generate_webpassword} if exists $data->{generate_webpassword};
        $subsciber_username_prefix = $data->{subsciber_username_prefix} if exists $data->{subsciber_username_prefix};

        $provision_subscriber_multithreading = $data->{provision_subscriber_multithreading} if exists $data->{provision_subscriber_multithreading};
        $provision_subscriber_numofthreads = _get_import_numofthreads($cpucount,$data,'provision_subscriber_numofthreads');

        $set_barring_profiles_multithreading = $data->{set_barring_profiles_multithreading} if exists $data->{set_barring_profiles_multithreading};
        $set_barring_profiles_numofthreads = _get_import_numofthreads($cpucount,$data,'set_barring_profiles_numofthreads');
        $barring_profiles_yml = $data->{barring_profiles_yml} if exists $data->{barring_profiles_yml};

        $set_peer_auth_multithreading = $data->{set_peer_auth_multithreading} if exists $data->{set_peer_auth_multithreading};
        $set_peer_auth_numofthreads = _get_import_numofthreads($cpucount,$data,'set_peer_auth_numofthreads');
        $peer_auth_realm = $data->{peer_auth_realm} if exists $data->{peer_auth_realm};

        $set_allowed_ips_multithreading = $data->{set_peer_auth_multithreading} if exists $data->{set_allowed_ips_multithreading};
        $set_allowed_ips_numofthreads = _get_import_numofthreads($cpucount,$data,'set_allowed_ips_numofthreads');
        $allowed_ips = [ split_tuple($data->{allowed_ips}) ] if exists $data->{allowed_ips};
        foreach my $ipnet (@$allowed_ips) {
            if (not check_ipnet($ipnet)) {
                configurationerror($configfile,"invalid allowed_ip '$ipnet'",getlogger(__PACKAGE__));
                $result = 0;
            }
        }

        $set_call_forwards_multithreading = $data->{set_call_forwards_multithreading} if exists $data->{set_call_forwards_multithreading};
        $set_call_forwards_numofthreads = _get_import_numofthreads($cpucount,$data,'set_call_forwards_numofthreads');
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

        $create_lnps_multithreading = $data->{create_lnps_multithreading} if exists $data->{create_lnps_multithreading};
        $create_lnps_numofthreads = _get_import_numofthreads($cpucount,$data,'create_lnps_numofthreads');
        $create_lnp_block_txn = $data->{create_lnp_block_txn} if exists $data->{create_lnp_block_txn};

        $set_preference_bulk_multithreading = $data->{set_preference_bulk_multithreading} if exists $data->{set_preference_bulk_multithreading};
        $set_preference_bulk_numofthreads = _get_import_numofthreads($cpucount,$data,'set_preference_bulk_numofthreads');
        $concurrent_max_total = $data->{concurrent_max_total} if exists $data->{concurrent_max_total};
        if (defined $concurrent_max_total and $concurrent_max_total <= 0) {
            configurationerror($configfile,'empty concurrent_max_total or greater than 0 required',getlogger(__PACKAGE__));
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
            $barring_profiles = $data->[0]->{'mapping'};
        };
        if ($@ or 'HASH' ne ref $barring_profiles or (scalar keys %$barring_profiles) == 0) {
            $barring_profiles //= {};
            configurationerror($configfile,'no barring profile mappings found',getlogger(__PACKAGE__));
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
    ($path_result,$rollback_path) = create_path($working_path . 'rollback',$rollback_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

sub _get_import_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $import_numofthreads = $default_value;
    $import_numofthreads = $data->{$key} if exists $data->{$key};
    $import_numofthreads = $cpucount if $import_numofthreads > $cpucount;
    return $import_numofthreads;
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
