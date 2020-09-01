use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::ETL::Lnp::Settings qw(
    update_settings

    check_dry

    $defaultsettings
    $defaultconfig
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
use NGCP::BulkProcessor::Mail qw(
    cleanupmsgfiles
);

use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);

use NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers qw();

use NGCP::BulkProcessor::Projects::ETL::Lnp::ProjectConnectorPool qw(destroy_all_dbs);

use NGCP::BulkProcessor::Projects::ETL::Lnp::Import qw(
    load_file
);

use NGCP::BulkProcessor::Projects::ETL::Lnp::ProcessLnp qw(
    create_lnp_numbers
    delete_lnp_numbers
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $load_file_task_opt = 'load_file';
push(@TASK_OPTS,$load_file_task_opt);

my $create_lnp_task_opt = 'create_lnp';
push(@TASK_OPTS,$create_lnp_task_opt);

my $delete_lnp_task_opt = 'delete_lnp';
push(@TASK_OPTS,$delete_lnp_task_opt);

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
    $result &= load_config($settingsfile,\&update_settings,$YAML_CONFIG_TYPE);
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

            if (lc($cleanup_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,1) if taskinfo($cleanup_task_opt,$result);

            } elsif (lc($load_file_task_opt) eq lc($task)) {
                $result &= load_file_task(\@messages) if taskinfo($load_file_task_opt,$result);

            } elsif (lc($create_lnp_task_opt) eq lc($task)) {
                if (taskinfo($create_lnp_task_opt,$result,1)) {
                    next unless check_dry();
                    $result &= create_lnp_task(\@messages);
                    $completion |= 1;
                }
                
            } elsif (lc($delete_lnp_task_opt) eq lc($task)) {
                if (taskinfo($delete_lnp_task_opt,$result,1)) {
                    next unless check_dry();
                    $result &= delete_lnp_task(\@messages);
                    $completion |= 1;
                }                

            } else {
                $result = 0;
                scripterror("unknown task option '" . $task . "', must be one of " . join(', ',@TASK_OPTS),getlogger(getscriptpath()));
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
            #cleanupcvsdirs() if $clean_generated;
            cleanupdbfiles() if $clean_generated;
            cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
            cleanupmsgfiles(\&fileerror,\&filewarn);
            #cleanupcertfiles();
            #cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
            $result = 1;
        };
    }
    if ($@ or !$result) {
        #print $@;
        push(@$messages,'working directory cleanup INCOMPLETE');
        return 0;
    } else {
        push(@$messages,'working directory folders cleaned up');
        return 1;
    }
}

sub load_file_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = load_file();
    };
    #print $@;
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total file LNP records: " .
            NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::countby_delta() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"loading LNP file INCOMPLETE$stats");
    } else {
        push(@$messages,"loading LNP file completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub create_lnp_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = create_lnp_numbers();
    };
    #print $@;
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total mariadb LNP providers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::countby_name() . ' rows';
        $stats .= "\n  total mariadb LNP numbers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::countby_lnpproviderid_number() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"creating LNP numbers INCOMPLETE$stats");
    } else {
        push(@$messages,"creating LNP numbers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return 1; #$result;

}

sub delete_lnp_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = delete_lnp_numbers();
    };
    #print $@;
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total mariadb LNP providers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers::countby_name() . ' rows';
        $stats .= "\n  total mariadb LNP numbers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::lnp_numbers::countby_lnpproviderid_number() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"deleting LNP numbers INCOMPLETE$stats");
    } else {
        push(@$messages,"deleting LNP numbers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return 1; #$result;

}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
