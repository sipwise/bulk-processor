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
    check_dry
    $output_path
    $rollback_path
    $defaultsettings
    $defaultconfig
    $dry
    $force
    $run_id
    $features_define_filename
    $subscriber_define_filename
    $lnp_define_filename
    $user_password_filename
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
);
use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(getscriptpath prompt cleanupdir);
use NGCP::BulkProcessor::Mail qw(
    cleanupmsgfiles
    wrap_mailbody
    $signature
    $normalpriority
    $lowpriority
    $highpriority
);
use NGCP::BulkProcessor::SqlConnectors::CSVDB qw(cleanupcvsdirs);
use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(destroy_all_dbs);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Import qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
    import_user_password
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];
my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);
my $import_features_define_task_opt = 'import_features';
push(@TASK_OPTS,$import_features_define_task_opt);
my $import_truncate_features_task_opt = 'truncate_features';
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
        "force" => \$force,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    #$result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    return $result;

}

sub main() {

    my @messages = ();
    my @attachmentfiles = ();
    my $result = 1;
    my $completion = 0;

    if ('ARRAY' eq ref $tasks and (scalar @$tasks) > 0) {
        foreach my $task (@$tasks) {

            if (lc($cleanup_task_opt) eq lc($task)) {
                $result = cleanup_task(\@messages,0) if taskinfo($cleanup_task_opt,$result);
            } elsif (lc($cleanup_all_task_opt) eq lc($task)) {
                $result = cleanup_task(\@messages,1) if taskinfo($cleanup_all_task_opt,$result);

            } elsif (lc($import_features_define_task_opt) eq lc($task)) {
                $result = import_features_define_task(\@messages) if taskinfo($import_features_define_task_opt,$result);
            } elsif (lc($import_truncate_features_task_opt) eq lc($task)) {
                $result = import_truncate_features_task(\@messages) if taskinfo($import_truncate_features_task_opt,$result);

            } elsif (lc($import_subscriber_define_task_opt) eq lc($task)) {
                $result = import_subscriber_define_task(\@messages) if taskinfo($import_subscriber_define_task_opt,$result);
            } elsif (lc($import_truncate_subscriber_task_opt) eq lc($task)) {
                $result = import_truncate_subscriber_task(\@messages) if taskinfo($import_truncate_subscriber_task_opt,$result);

            } elsif (lc($import_lnp_define_task_opt) eq lc($task)) {
                $result = import_lnp_define_task(\@messages) if taskinfo($import_lnp_define_task_opt,$result);
            } elsif (lc($import_truncate_lnp_task_opt) eq lc($task)) {
                $result = import_truncate_lnp_task(\@messages) if taskinfo($import_truncate_lnp_task_opt,$result);

            } elsif (lc($import_user_password_task_opt) eq lc($task)) {
                $result = import_user_password_task(\@messages) if taskinfo($import_user_password_task_opt,$result);
            } elsif (lc($import_truncate_user_password_task_opt) eq lc($task)) {
                $result = import_truncate_user_password_task(\@messages) if taskinfo($import_truncate_user_password_task_opt,$result);

            } elsif (lc('blah') eq lc($task)) {
                if (taskinfo($cleanup_task_opt,$result)) {
                    next unless check_dry();
                    $result = import_features_define_task(\@messages);
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
    my ($task,$result) = @_;
    scriptinfo($result ? "starting task: '$task'" : "skipping task '$task' due to previous problems",getlogger(getscriptpath()));
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
    my $result = 0;
    eval {
        $result = import_features_define($features_define_filename);
    };
    my $err = $@;
    my $stats = '';
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
    my $result = 0;
    eval {
        $result = import_subscriber_define($subscriber_define_filename);
    };
    my $err = $@;
    my $stats = '';
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
    my $result = 0;
    eval {
        $result = import_lnp_define($lnp_define_filename);
    };
    my $err = $@;
    my $stats = '';
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

        $stats .= "\n  total lrn codes: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::count_lrncodes();
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
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_lrncode_portednumber() . ' rows';
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
    my $result = 0;
    eval {
        $result = import_user_password($user_password_filename);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total username password records: " .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn() . ' rows';

        my $added_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
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
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_lrncode_portednumber() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported username passwords INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported username passwords completed$stats");
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
