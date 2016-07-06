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

use NGCP::BulkProcessor::ConnectorPool qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Import qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];
my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);
my $import_features_define_task_opt = 'import_features';
push(@TASK_OPTS,$import_features_define_task_opt);
my $import_subscriber_define_task_opt = 'import_subscriber';
push(@TASK_OPTS,$import_subscriber_define_task_opt);
my $import_lnp_define_task_opt = 'import_lnp';
push(@TASK_OPTS,$import_lnp_define_task_opt);


if (init()) {
    main();
    exit(0);
} else {
    exit(1);
}

sub init {

    my $configfile = $defaultconfig;
    my $settingsfile = $defaultsettings;

    GetOptions ("config=s" => \$configfile,
                "settings=s" => \$settingsfile,
                "task=s" => $tasks,
                "run=s" => \$run_id,
                "dry" => \$dry,
                "force" => \$force,
    ) or scripterror('error in command line arguments',getlogger(getscriptpath()));

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
            } elsif (lc($import_subscriber_define_task_opt) eq lc($task)) {
                $result = import_subscriber_define_task(\@messages) if taskinfo($import_subscriber_define_task_opt,$result);
            } elsif (lc($import_lnp_define_task_opt) eq lc($task)) {
                $result = import_lnp_define_task(\@messages) if taskinfo($import_lnp_define_task_opt,$result);
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
    eval {
        cleanupcvsdirs() if $clean_generated;
        cleanupdbfiles() if $clean_generated;
        cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
        cleanupmsgfiles(\&fileerror,\&filewarn);
        cleanupdir($output_path,0,\&scriptinfo,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
        cleanupdir($rollback_path,0,\&scriptinfo,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
    };
    if ($@) {
        push(@$messages,'working directory cleanup incomplete');
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
    if ($@ or !$result) {
        push(@$messages,'importing features incomplete');
    } else {
        push(@$messages,'importing features completed: xy records');
        destroy_dbs(); #every task should leave with closed connections.
    }
    return $result;

}

sub import_subscriber_define_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = import_subscriber_define($subscriber_define_filename);
    };
    if ($@ or !$result) {
        push(@$messages,'importing subscribers incomplete');
    } else {
        push(@$messages,'importing subscribers completed: xy records');
        destroy_dbs(); #every task should leave with closed connections.
    }
    return $result;

}

sub import_lnp_define_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = import_lnp_define($lnp_define_filename);
    };
    if ($@ or !$result) {
        push(@$messages,'importing lnp incomplete');
    } else {
        push(@$messages,'importing lnp completed: xy records');
        destroy_dbs(); #every task should leave with closed connections.
    }
    return $result;

}

sub destroy_dbs() {
    NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool::destroy_dbs();
    NGCP::BulkProcessor::ConnectorPool::destroy_dbs();
}

#END {
#    # this should not be required explicitly, but prevents Log4Perl's
#    # "rootlogger not initialized error upon exit..
#    NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool::destroy_dbs();
#    NGCP::BulkProcessor::ConnectorPool::destroy_dbs();
#}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
