use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Settings qw(
    update_settings
    update_mappings
    check_dry

    $defaultsettings
    $defaultconfig
    $mappings_yml
    $dry
    $skip_errors
    $force

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
use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw(cleanupcertfiles);
use NGCP::BulkProcessor::Mail qw(
    cleanupmsgfiles
);

#use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();

#use NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Contracts qw(
#    set_profile_package
#);

use NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Api qw(
    set_profile_package
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $set_profile_package_task_opt = 'set_profile_package';
push(@TASK_OPTS,$set_profile_package_task_opt);

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

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
        "dry" => \$dry,
        "skip-errors" => \$skip_errors,
        "force" => \$force,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($mappings_yml,\&update_mappings,$YAML_CONFIG_TYPE);
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

            if (lc($set_profile_package_task_opt) eq lc($task)) {
                if (taskinfo($set_profile_package_task_opt,$result)) {
                    next unless check_dry();
                    $result &= set_profile_package_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($cleanup_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages) if taskinfo($cleanup_task_opt,$result);

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
    my ($messages) = @_;
    my $result = 0;
    eval {
        #cleanupcvsdirs() if $clean_generated;
        #cleanupdbfiles() if $clean_generated;
        cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
        cleanupmsgfiles(\&fileerror,\&filewarn);
        cleanupcertfiles();
        #cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
        #cleanupdir($rollback_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
        $result = 1;
    };

    if ($@ or !$result) {
        push(@$messages,'cleanup INCOMPLETE');
        return 0;
    } else {
        push(@$messages,'cleanup completed');
        return 1;
    }
}

sub set_profile_package_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = set_profile_package();
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    #eval {

    #};
    if ($err or !$result) {
        push(@$messages,"set profile package INCOMPLETE$stats");
    } else {
        push(@$messages,"set profile package completed$stats");
    }
    destroy_dbs(); #every task should leave with closed connections.
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