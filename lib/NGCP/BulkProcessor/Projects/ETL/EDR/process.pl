use strict;

## no critic

our $VERSION = "0.0";

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::ETL::EDR::Settings qw(
    update_settings

    get_export_filename
    $subscriber_profiles_export_filename_format

    check_dry
    $output_path
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

use NGCP::BulkProcessor::SqlConnectors::CSVDB qw(cleanupcvsdirs);
use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);

use NGCP::BulkProcessor::Projects::ETL::EDR::ProjectConnectorPool qw(destroy_all_dbs get_csv_db get_sqlite_db);

use NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents qw();

use NGCP::BulkProcessor::Projects::ETL::EDR::ExportEvents qw(
    export_subscriber_profiles
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB;

my @TASK_OPTS = ();

my $tasks = [];

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $export_subscriber_profiles_task_opt = 'export_subscriber_profiles';
push(@TASK_OPTS,$export_subscriber_profiles_task_opt);

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
        "skip-errors" => \$skip_errors,
        "force" => \$force,
    );

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
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

            } elsif (lc($export_subscriber_profiles_task_opt) eq lc($task)) {
                $result &= export_subscriber_profiles_task(\@messages) if taskinfo($export_subscriber_profiles_task_opt,$result);
                $completion |= 1;

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
            cleanupcvsdirs();
            cleanupdbfiles();
            cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
            cleanupmsgfiles(\&fileerror,\&filewarn);
            cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
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

sub export_subscriber_profiles_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = export_subscriber_profiles();
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        my ($export_filename,$export_format) = get_export_filename($subscriber_profiles_export_filename_format);
        if ('sqlite' eq $export_format) {
            &get_sqlite_db()->copydbfile($export_filename);    
        } elsif ('csv' eq $export_format) {
            NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents::copy_table(\&get_csv_db);
            &get_csv_db()->copytablefile(NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents::gettablename(),$export_filename);
        } else {
            push(@$messages,'invalid extension for output filename $export_filename');
        }
    };
    if ($err or !$result) {
        push(@$messages,"exporting subscriber profiles INCOMPLETE$stats");
    } else {
        push(@$messages,"exporting subscriber profiles completed$stats");
    }
    destroy_all_dbs(); 
    return $result;

}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
