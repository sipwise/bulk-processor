use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
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
use NGCP::BulkProcessor::SqlConnectors::CSVDB qw(cleanupcvsdirs);
use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(destroy_all_dbs);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Check qw(
    check_billing_db_tables
    check_provisioning_db_tables
    check_kamailio_db_tables
    check_import_db_tables
    check_rest_get_items
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Import qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
    import_user_password
    import_batch
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Provisioning qw(
    provision_subscribers
    provision_subscribers_batch

    update_webpasswords
    update_webpasswords_batch
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Preferences qw(
    set_barring_profiles
    set_barring_profiles_batch

    set_peer_auth
    set_peer_auth_batch

    set_allowed_ips
    set_allowed_ips_batch

    set_preference_bulk
    set_preference_bulk_batch

    $INIT_PEER_AUTH_MODE
    $SWITCHOVER_PEER_AUTH_MODE
    $CLEAR_PEER_AUTH_MODE
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Api qw(
    set_call_forwards
    set_call_forwards_batch
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Lnp qw(
    create_lnps
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $check_task_opt = 'check';
push(@TASK_OPTS,$check_task_opt);

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $import_features_define_task_opt = 'import_feature';
push(@TASK_OPTS,$import_features_define_task_opt);
my $import_truncate_features_task_opt = 'truncate_feature';
push(@TASK_OPTS,$import_truncate_features_task_opt);

my $import_subscriber_define_task_opt = 'import_subscriber';
push(@TASK_OPTS,$import_subscriber_define_task_opt);
my $import_truncate_subscriber_task_opt = 'truncate_subscriber';
push(@TASK_OPTS,$import_truncate_subscriber_task_opt);

my $import_lnp_define_task_opt = 'import_lnp';
push(@TASK_OPTS,$import_lnp_define_task_opt);
my $import_truncate_lnp_task_opt = 'truncate_lnp';
push(@TASK_OPTS,$import_truncate_lnp_task_opt);

my $import_user_password_task_opt = 'import_user_password';
push(@TASK_OPTS,$import_user_password_task_opt);
my $import_truncate_user_password_task_opt = 'truncate_user_password';
push(@TASK_OPTS,$import_truncate_user_password_task_opt);

my $import_batch_task_opt = 'import_batch';
push(@TASK_OPTS,$import_batch_task_opt);
my $import_truncate_batch_task_opt = 'truncate_batch';
push(@TASK_OPTS,$import_truncate_batch_task_opt);

my $provision_subscriber_task_opt = 'provision_subscriber';
push(@TASK_OPTS,$provision_subscriber_task_opt);

my $set_barring_profiles_task_opt = 'set_barring_profiles';
push(@TASK_OPTS,$set_barring_profiles_task_opt);

my $init_peer_auth_task_opt = 'init_peer_auth';
push(@TASK_OPTS,$init_peer_auth_task_opt);

my $switchover_peer_auth_task_opt = 'switchover_peer_auth';
push(@TASK_OPTS,$switchover_peer_auth_task_opt);

my $clear_peer_auth_task_opt = 'clear_peer_auth';
push(@TASK_OPTS,$clear_peer_auth_task_opt);

my $set_allowed_ips_task_opt = 'set_allowed_ips';
push(@TASK_OPTS,$set_allowed_ips_task_opt);

my $set_call_forwards_task_opt = 'set_call_forwards';
push(@TASK_OPTS,$set_call_forwards_task_opt);

my $set_concurrent_max_total_task_opt = 'set_concurrent_max_total';
push(@TASK_OPTS,$set_concurrent_max_total_task_opt);

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
        "batch" => \$batch,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($barring_profiles_yml,\&update_barring_profiles,$YAML_CONFIG_TYPE);
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

            } elsif (lc($cleanup_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,0) if taskinfo($cleanup_task_opt,$result);
            } elsif (lc($cleanup_all_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,1) if taskinfo($cleanup_all_task_opt,$result);

            } elsif (lc($import_features_define_task_opt) eq lc($task)) {
                $result &= import_features_define_task(\@messages) if taskinfo($import_features_define_task_opt,$result);
            } elsif (lc($import_truncate_features_task_opt) eq lc($task)) {
                $result &= import_truncate_features_task(\@messages) if taskinfo($import_truncate_features_task_opt,$result);

            } elsif (lc($import_subscriber_define_task_opt) eq lc($task)) {
                $result &= import_subscriber_define_task(\@messages) if taskinfo($import_subscriber_define_task_opt,$result);
            } elsif (lc($import_truncate_subscriber_task_opt) eq lc($task)) {
                $result &= import_truncate_subscriber_task(\@messages) if taskinfo($import_truncate_subscriber_task_opt,$result);

            } elsif (lc($import_lnp_define_task_opt) eq lc($task)) {
                $result &= import_lnp_define_task(\@messages) if taskinfo($import_lnp_define_task_opt,$result);
            } elsif (lc($import_truncate_lnp_task_opt) eq lc($task)) {
                $result &= import_truncate_lnp_task(\@messages) if taskinfo($import_truncate_lnp_task_opt,$result);

            } elsif (lc($import_user_password_task_opt) eq lc($task)) {
                $result &= import_user_password_task(\@messages) if taskinfo($import_user_password_task_opt,$result);
            } elsif (lc($import_truncate_user_password_task_opt) eq lc($task)) {
                $result &= import_truncate_user_password_task(\@messages) if taskinfo($import_truncate_user_password_task_opt,$result);

            } elsif (lc($import_batch_task_opt) eq lc($task)) {
                $result &= import_batch_task(\@messages) if taskinfo($import_batch_task_opt,$result);
            } elsif (lc($import_truncate_batch_task_opt) eq lc($task)) {
                $result &= import_truncate_batch_task(\@messages) if taskinfo($import_truncate_batch_task_opt,$result);

            } elsif (lc($provision_subscriber_task_opt) eq lc($task)) {
                if (taskinfo($provision_subscriber_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= provision_subscriber_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($set_barring_profiles_task_opt) eq lc($task)) {
                if (taskinfo($set_barring_profiles_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_barring_profiles_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($init_peer_auth_task_opt) eq lc($task)) {
                if (taskinfo($init_peer_auth_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_peer_auth_task(\@messages,$INIT_PEER_AUTH_MODE);
                    $completion |= 1;
                }

            } elsif (lc($switchover_peer_auth_task_opt) eq lc($task)) {
                if (taskinfo($switchover_peer_auth_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_peer_auth_task(\@messages,$SWITCHOVER_PEER_AUTH_MODE);
                    $completion |= 1;
                }

            } elsif (lc($clear_peer_auth_task_opt) eq lc($task)) {
                if (taskinfo($clear_peer_auth_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_peer_auth_task(\@messages,$CLEAR_PEER_AUTH_MODE);
                    $completion |= 1;
                }

            } elsif (lc($set_allowed_ips_task_opt) eq lc($task)) {
                if (taskinfo($set_allowed_ips_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_allowed_ips_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($set_call_forwards_task_opt) eq lc($task)) {
                if (taskinfo($set_call_forwards_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_call_forwards_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($set_concurrent_max_total_task_opt) eq lc($task)) {
                if (taskinfo($set_concurrent_max_total_task_opt,$result,1) and ($result = batchinfo($result))) {
                    next unless check_dry();
                    $result &= set_preference_bulk_task(\@messages,
                        $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CONCURRENT_MAX_TOTAL_ATTRIBUTE,
                        $concurrent_max_total);
                    $completion |= 1;
                }

            } elsif (lc($create_lnps_task_opt) eq lc($task)) {
                if (taskinfo($create_lnps_task_opt,$result)) {
                    next unless check_dry();
                    $result &= create_lnps_task(\@messages);
                    $completion |= 1;
                }

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
    my ($task,$result,$batch_supported) = @_;
    scriptinfo($result ? "starting task: '$task'" : "skipping task '$task' due to previous problems",getlogger(getscriptpath()));
    if (!$batch_supported and $batch) {
        scriptwarn("no batch processing supported for this mode",getlogger(getscriptpath()));
    }
    return $result;
}

sub batchinfo {

    my ($result) = @_;
    if ($result) {
        if ($batch) {
            $result = 0;
            my $stats = '';
            eval {
                my $batch_size = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta({ 'NOT IN' =>
                        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta});
                $stats .= " of $batch_size subscriber number(s)";
                $result = ($batch_size > 0 ? 1 : 0);
            };
            if ($@ or not $result) {
                destroy_all_dbs();
                scriptwarn("processing is limited to batch$stats (you might need to import a non-empty batch first)",getlogger(getscriptpath()));
            } else {
                scriptinfo("processing is limited to batch$stats",getlogger(getscriptpath()));
            }
        } else {
            $result = 1;
        }
    }
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

    @check_messages = ();
    $result = check_kamailio_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_rest_get_items(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_import_db_tables(\@check_messages);
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

sub import_features_define_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_features_define($features_define_filename);
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  total feature option records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";

        $stats .= "\n  total feature set option item records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_subscribernumber_option_optionsetitem() . ' rows';

        $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";

    };
    if ($err or !$result) {
        push(@$messages,"importing subscriber features INCOMPLETE$stats");
    } else {
        push(@$messages,"importing subscriber features completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_features_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::create_table(1);
        $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total feature option records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option() . ' rows';
        $stats .= "\n  total feature set option item records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_subscribernumber_option_optionsetitem() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported subscriber features INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported subscriber features completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_subscriber_define_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_subscriber_define($subscriber_define_filename);
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  total subscriber records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"importing subscribers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_subscriber_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total subscriber records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported subscribers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_lnp_define_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_lnp_define($lnp_define_filename);
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  total lnp number records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_lrncode_portednumber() . ' rows';

        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";

        my $lrn_codes = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::list_lrncodes_delta();
        $stats .= "\n  lrn codes: " . (scalar @$lrn_codes) . ' rows';
        foreach my $lrn_code (@$lrn_codes) {
            $stats .= "\n    '" . $lrn_code->{lrn_code} . "': " . $lrn_code->{delta};
        }
    };
    if ($err or !$result) {
        push(@$messages,"importing lnp numbers INCOMPLETE$stats");
    } else {
        push(@$messages,"importing lnp numbers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_lnp_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total lnp number records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_lrncode_portednumber() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported lnp numbers INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported lnp numbers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_user_password_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_user_password($user_password_filename);
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  total username password records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn() . ' rows';

        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $unchanged_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::unchanged_delta
        );
        $stats .= "\n    unchanged: $unchanged_count rows";
        my $updated_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta
        );
        $stats .= "\n    updated: $updated_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing username passwords INCOMPLETE$stats");
    } else {
        push(@$messages,"importing username passwords completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_user_password_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total username password records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported username passwords INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported username passwords completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_batch_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_batch($batch_filename);
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  total batch records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_number() . ' rows';

        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing batch INCOMPLETE$stats");
    } else {
        push(@$messages,"importing batch completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_batch_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total batch records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_number() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported batch records INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported batch records completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}



sub provision_subscriber_task {

    my ($messages) = @_;
    my ($result,$warning_count,$updated_password_count) = (0,0,0);
    eval {
        if ($batch) {
            ($result,$warning_count,$updated_password_count) = provision_subscribers_batch();
        } else {
            ($result,$warning_count,$updated_password_count) = provision_subscribers();
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    my $updated_password_count = 0;
    eval {
        $stats .= "\n  total contracts: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_status_resellerid(undef,$reseller_id) . ' rows';
        my $active_count = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_status_resellerid(
            $NGCP::BulkProcessor::Dao::Trunk::billing::contracts::ACTIVE_STATE,
            $reseller_id
        );
        $stats .= "\n    active: $active_count rows";
        my $terminated_count = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_status_resellerid(
            $NGCP::BulkProcessor::Dao::Trunk::billing::contracts::TERMINATED_STATE,
            $reseller_id
        );
        $stats .= "\n    terminated: $terminated_count rows";

        $stats .= "\n  total subscribers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(undef,$reseller_id) . ' rows';
        $active_count = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(
            $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::ACTIVE_STATE,
            $reseller_id
        );
        $stats .= "\n    active: $active_count rows";
        $terminated_count = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(
            $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE,
            $reseller_id
        );
        $stats .= "\n    terminated: $terminated_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"provisioning subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"provisioning subscribers completed$stats");
        if (not $dry and $reprovision_upon_password_change and $updated_password_count > 0) {
            push(@$messages,"THERE WERE $updated_password_count UPDATED PASSWORDS. YOU MIGHT WANT TO RESTART SEMS NOW ...");
        }
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

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


sub set_peer_auth_task {

    my ($messages,$mode) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($batch) {
            ($result,$warning_count) = set_peer_auth_batch($mode);
        } else {
            ($result,$warning_count) = set_peer_auth($mode);
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        my $peer_auth_user_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::PEER_AUTH_USER);
        my $peer_auth_pass_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::PEER_AUTH_PASS);
        my $peer_auth_realm_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::PEER_AUTH_REALM);
        my $peer_auth_register_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::PEER_AUTH_REGISTER);
        my $force_inbound_calls_to_peer_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::FORCE_INBOUND_CALLS_TO_PEER);

        $stats .= "\n  '" . $peer_auth_user_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $peer_auth_user_attribute->{id},undef) . ' rows';
        $stats .= "\n  '" . $peer_auth_pass_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $peer_auth_pass_attribute->{id},undef) . ' rows';
        $stats .= "\n  '" . $peer_auth_realm_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $peer_auth_realm_attribute->{id},undef) . ' rows';

        $stats .= "\n  '" . $peer_auth_register_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $peer_auth_register_attribute->{id},undef) . ' rows';
        $stats .= "\n  '" . $force_inbound_calls_to_peer_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $force_inbound_calls_to_peer_attribute->{id},undef) . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"$mode subscribers\' peer auth preference INCOMPLETE$stats");
    } else {
        push(@$messages,"$mode subscribers\' peer auth preference completed$stats");
        if (not $dry) {
            push(@$messages,"YOU MIGHT WANT TO RESTART SEMS NOW ...");
        }
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub set_allowed_ips_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($batch) {
            ($result,$warning_count) = set_allowed_ips_batch();
        } else {
            ($result,$warning_count) = set_allowed_ips();
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {

        my $allowed_ips_grp_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE);
        $stats .= "\n  '" . $allowed_ips_grp_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $allowed_ips_grp_attribute->{id},undef) . ' rows';
        foreach my $ipnet (@$allowed_ips) {
            $stats .= "\n    '$ipnet': " . NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups::countby_groupid_ipnet(undef,$ipnet) . ' rows';
        }
        $stats .= "\n  voip_aig_sequence: " . NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence::get_id();

    };
    if ($err or !$result) {
        push(@$messages,"set subscribers\' allowed_ips preference INCOMPLETE$stats");
    } else {
        push(@$messages,"set subscribers\' allowed_ips preference completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub set_call_forwards_task {

    my ($messages,$mode) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($batch) {
            ($result,$warning_count) = set_call_forwards_batch($mode);
        } else {
            ($result,$warning_count) = set_call_forwards($mode);
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        $stats .= "\n  '" . $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFU_TYPE . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,
                $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFU_TYPE) . ' rows';

        $stats .= "\n  '" . $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFB_TYPE . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,
                $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFB_TYPE) . ' rows';

        $stats .= "\n  '" . $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFT_TYPE . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,
                $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFT_TYPE) . ' rows';

        $stats .= "\n  '" . $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFNA_TYPE . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,
                $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::CFNA_TYPE) . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"set subscribers\' call forwards INCOMPLETE$stats");
    } else {
        push(@$messages,"set subscribers\' call forwards completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub create_lnps_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = create_lnps();
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    my $lnp_providers = [];
    eval {
        $lnp_providers = NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::findby_prefix();
    };
    if ($@) {
        eval {
            $lnp_providers = NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers::findby_prefix();
        };
    }
    eval {

        $stats .= "\n  lnp_numbers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::countby_lnpproviderid_number() . ' rows';

        foreach my $lnp_provider (@$lnp_providers) {
            $stats .= "\n    '" . $lnp_provider->{name} . "': " .
                NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::countby_lnpproviderid_number($lnp_provider->{id}) . ' rows';
        }

    };
    if ($err or !$result) {
        push(@$messages,"create lnps INCOMPLETE$stats");
    } else {
        push(@$messages,"create lnps completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub set_preference_bulk_task {

    my ($messages,$bulk_attribute_name,$value) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($batch) {
            ($result,$warning_count) = set_preference_bulk_batch($bulk_attribute_name,$value);
        } else {
            ($result,$warning_count) = set_preference_bulk($bulk_attribute_name,$value);
        }
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {
        my $bulk_attribute = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute($bulk_attribute_name);

        $stats .= "\n  '" . $bulk_attribute->{attribute} . "': " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::countby_subscriberid_attributeid_value(undef,
                $bulk_attribute->{id},undef) . ' rows';

    };
    if ($err or !$result) {
        push(@$messages,"set subscribers\' $bulk_attribute_name preference INCOMPLETE$stats");
    } else {
        push(@$messages,"set subscribers\' $bulk_attribute_name preference completed$stats");
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
