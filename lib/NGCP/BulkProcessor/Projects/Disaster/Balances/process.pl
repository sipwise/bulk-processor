use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Disaster::Balances::Settings qw(
    update_settings
    update_barring_profiles
    check_dry
    $output_path
    $rollback_path
    $defaultsettings
    $defaultconfig
    $dry
    $skip_errors
    $force
    $batch
    $run_id
    $features_define_filename
    $subscriber_define_filename
    $lnp_define_filename
    $user_password_filename
    $batch_filename
    $reseller_id
    $barring_profiles_yml
    $barring_profiles
    $allowed_ips
    $concurrent_max_total
    $reprovision_upon_password_change
);
use NGCP::BulkProcessor::Logging qw(
    init_log
    getlogger
    $attachmentlogfile
    scriptinfo
    cleanuplogfiles
    $currentlogfile
);
use NGCP::BulkProcessor::LogError qw (
    completion
    done
    scriptwarn
    scripterror
    filewarn
    fileerror
);
use NGCP::BulkProcessor::LoadConfig qw(
    load_config
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
    $ANY_CONFIG_TYPE
);
use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(getscriptpath prompt cleanupdir);
use NGCP::BulkProcessor::Mail qw(
    cleanupmsgfiles
);
#use NGCP::BulkProcessor::SqlConnectors::CSVDB qw(cleanupcvsdirs);
#use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);

#use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(destroy_all_dbs);

#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
#use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

#use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
#use NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();

#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();

use NGCP::BulkProcessor::Projects::Disaster::Balances::Check qw(
    check_billing_db_tables
    check_rest_get_items
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Provisioning qw(
    provision_subscribers
    provision_subscribers_batch
);

use NGCP::BulkProcessor::Projects::Disaster::Balances::Api qw(
    set_call_forwards
    set_call_forwards_batch
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $check_task_opt = 'check';
push(@TASK_OPTS,$check_task_opt);

my $create_lnps_task_opt = 'create_lnps';
push(@TASK_OPTS,$create_lnps_task_opt);

if (init()) {
    main();
    exit(0);
} else {
    exit(1);
}

sub init {

    my $configfile = $defaultconfig;
    my $settingsfile = $defaultsettings;

    return 0 unless GetOptions(
        "config=s" => \$configfile,
        "settings=s" => \$settingsfile,
        "task=s" => $tasks,
        "run=s" => \$run_id,
        "dry" => \$dry,
        "skip-errors" => \$skip_errors,
        "force" => \$force,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    #$result &= load_config($barring_profiles_yml,\&update_barring_profiles,$YAML_CONFIG_TYPE);
    return $result;

}

sub main() {

    my @messages = ();
    my @attachmentfiles = ();
    my $result = 1;
    my $completion = 0;

    if (defined $tasks and 'ARRAY' eq ref $tasks and (scalar @$tasks) > 0) {
        scriptinfo('skip-errors: processing won\'t stop upon errors',getlogger(__PACKAGE__)) if $skip_errors;
        foreach my $task (@$tasks) {

            if (lc($check_task_opt) eq lc($task)) {
                $result &= check_task(\@messages) if taskinfo($check_task_opt,$result);


            } else {
                $result = 0;
                scripterror("unknow task option '" . $task . "', must be one of " . join(', ',@TASK_OPTS),getlogger(getscriptpath()));
                last;
            }
        }
    } else {
        $result = 0;
        scripterror('at least one task option is required. supported tasks: ' . join(', ',@TASK_OPTS),getlogger(getscriptpath()));
    }

    push(@attachmentfiles,$attachmentlogfile);
    if ($completion) {
        completion(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
    } else {
        done(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
    }

    return $result;
}

sub taskinfo {
    my ($task,$result) = @_;
    scriptinfo($result ? "starting task: '$task'" : "skipping task '$task' due to previous problems",getlogger(getscriptpath()));
    return $result;
}

sub check_task {
    my ($messages) = @_;
    my @check_messages = ();
    my $result = check_billing_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_provisioning_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));


    destroy_all_dbs();
    return $result;
}

sub cleanup_task {
    my ($messages,$clean_generated) = @_;
    my $result = 0;
    if (!$clean_generated or $force or 'yes' eq lc(prompt("Type 'yes' to proceed: "))) {
        eval {
            cleanupcvsdirs() if $clean_generated;
            cleanupdbfiles() if $clean_generated;
            cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
            cleanupmsgfiles(\&fileerror,\&filewarn);
            cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
            cleanupdir($rollback_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
            $result = 1;
        };
    }
    if ($@ or !$result) {
        push(@$messages,'working directory cleanup INCOMPLETE');
        return 0;
    } else {
        push(@$messages,'working directory folders cleaned up');
        return 1;
    }
}



sub set_barring_profiles_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($batch) {
            ($result,$warning_count) = set_barring_profiles_batch();
        } else {
            ($result,$warning_count) = set_barring_profiles();
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        my $adm_ncos_id_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ID_ATTRIBUTE);
        my $subscriber_barring_profiles = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::list_barringprofiles();
        foreach my $barring_profile (@$subscriber_barring_profiles) {
            if (exists $barring_profiles->{$barring_profile}) {
                my $level = $barring_profiles->{$barring_profile};
                if (defined $level and length($level) > 0) {
                    my $ncos_level = NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_resellerid_level(
                        $reseller_id,$level);
                    $stats .= "\n  '$barring_profile' / '" . $ncos_level->{level}. "': " .
                        NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                            $adm_ncos_id_attribute->{id},$ncos_level->{id}) . ' rows';

                }
            }
        }
    };
    if ($err or !$result) {
        push(@$messages,"set subscribers\' ncos level preference INCOMPLETE$stats");
    } else {
        push(@$messages,"set subscribers\' ncos level preference completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

#END {
#    # this should not be required explicitly, but prevents Log4Perl's
#    # "rootlogger not initialized error upon exit..
#    destroy_all_dbs
#}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
