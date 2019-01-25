use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Settings qw(
    $output_path
    $tempfile_path
);
use NGCP::BulkProcessor::Projects::Export::Ama::Ccs::Settings qw(
    $defaultsettings
    $defaultconfig
    $skip_errors
    $force
);
#$dry
#check_dry
#@provider_config
#@providers
#$providers_yml

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
use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw(cleanupcertfiles);

use NGCP::BulkProcessor::ConnectorPool qw(destroy_dbs);

#use NGCP::BulkProcessor::Projects::Massive::Generator::Dao::Blah qw();

#use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Ccs::CDR qw(
    export_cdrs
    reset_fsn
    reset_export_status
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];
my $from = undef,
my $to = undef;

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $export_cdr_task_opt = 'export_cdr';
push(@TASK_OPTS,$export_cdr_task_opt);

my $reset_fsn_task_opt = 'reset_fsn';
push(@TASK_OPTS,$reset_fsn_task_opt);

my $reset_export_status_task_opt = 'reset_export_status';
push(@TASK_OPTS,$reset_export_status_task_opt);

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
        #"dry" => \$dry,
        "skip-errors" => \$skip_errors,
        "force" => \$force,
        "from=s" => \$from,
        "to=s" => \$to,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&NGCP::BulkProcessor::Projects::Export::Ama::Format::Settings::update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($settingsfile,\&NGCP::BulkProcessor::Projects::Export::Ama::Ccs::Settings::update_settings,$SIMPLE_CONFIG_TYPE);
    #$result &= load_config($providers_yml,\&update_provider_config,$YAML_CONFIG_TYPE);

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
                $result &= cleanup_task(\@messages,0) if taskinfo($cleanup_task_opt,$result);
            } elsif (lc($cleanup_all_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,1) if taskinfo($cleanup_all_task_opt,$result);

            } elsif (lc($export_cdr_task_opt) eq lc($task)) {
                if (taskinfo($export_cdr_task_opt,$result,1)) {
                    #next unless check_dry();
                    $result &= export_cdr_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($reset_fsn_task_opt) eq lc($task)) {
                if (taskinfo($reset_fsn_task_opt,$result,1)) {
                    #next unless check_dry();
                    $result &= reset_fsn_task(\@messages);
                }

            } elsif (lc($reset_export_status_task_opt) eq lc($task)) {
                if (taskinfo($reset_export_status_task_opt,$result,1)) {
                    #next unless check_dry();
                    $result &= reset_export_status_task(\@messages);
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
            cleanupcertfiles();
            cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
            cleanupdir($tempfile_path,1,\&filewarn,getlogger(getscriptpath()));
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

sub export_cdr_task {

    my ($messages) = @_;
    my ($result,$warning_count,$ama_files) = (0,0,[]);
    eval {
       ($result,$warning_count,$ama_files) = export_cdrs();
    };
    my $err = $@;
    my $stats = ": " . (scalar @$ama_files) . ' files'; # . ((scalar @$ama_files) > 0 ? "\n  " : '') . join("\n  ",@$ama_files);
    foreach my $ama_file (@$ama_files) {
        $stats .= "\n  " . $ama_file;
    }
    #eval {
    #    #stats .= "\n  total CDRs: " .
    #    #    NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::countby_ratingstatus(undef) . ' rows';
    #};
    if ($err or !$result) {
        push(@$messages,"export cdrs INCOMPLETE$stats");
    } else {
        push(@$messages,"export cdrs completed$stats");
    }
    destroy_dbs();
    return $result;

}

sub reset_fsn_task {

    my ($messages) = @_;
    my ($result) = (0);
    eval {
        ($result) = reset_fsn();
    };
    my $err = $@;
    if ($err or !$result) {
        push(@$messages,"reset file sequence number INCOMPLETE");
    } else {
        push(@$messages,"reset file sequence number completed");
    }
    destroy_dbs();
    return $result;

}

sub reset_export_status_task {

    my ($messages) = @_;
    my ($result) = (0);
    eval {
        ($result) = reset_export_status($from,$to);
    };
    my $err = $@;
    my $fromto = 'from ' . ($from ? $from : '-') . ' to ' . ($to ? $to : '-');
    if ($err or !$result) {
        push(@$messages,"reset export status $fromto INCOMPLETE");
    } else {
        push(@$messages,"reset export status $fromto completed");
    }
    destroy_dbs();
    return $result;

}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
