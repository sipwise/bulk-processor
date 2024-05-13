use strict;

## no critic

our $VERSION = "0.0";

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Settings qw(
    update_settings
    
    $output_path
    $defaultsettings
    $defaultconfig

    $skip_errors
    $force
    
    get_export_filename
    $registrations_export_filename_format
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

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location qw();

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::ProjectConnectorPool qw(
    get_sqlite_db
    get_csv_db
    destroy_all_dbs
);
use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_stores
);

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Process qw(
    load_registrations_file
    load_registrations_all
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];
my $file = undef;

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $load_registrations_task_opt = 'load_registrations';
push(@TASK_OPTS,$load_registrations_task_opt);

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
        "file=s" => \$file,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

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

            } elsif (lc($load_registrations_task_opt) eq lc($task)) {
                $result &= load_registrations_task(\@messages) if taskinfo($load_registrations_task_opt,$result);

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
            #cleanupcsvdirs() if $clean_generated;
            cleanupdbfiles() if $clean_generated;
            cleanuplogfiles(\&fileerror,\&filewarn,($currentlogfile,$attachmentlogfile));
            cleanupmsgfiles(\&fileerror,\&filewarn);
            #cleanupcertfiles();
            cleanupdir($output_path,1,\&filewarn,getlogger(getscriptpath())) if $clean_generated;
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

sub load_registrations_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        if ($file) {
            ($result,$warning_count) = load_registrations_file($file);
        } else {
            ($result,$warning_count) = load_registrations_all();
        }
    };
    #print $@;
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total loaded registrations: " .
            NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::countby_delta() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::countby_delta(
            $NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::countby_delta(
            $NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::countby_delta(
            $NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
        my ($export_filename,$export_format) = get_export_filename($registrations_export_filename_format);
        if ('sqlite' eq $export_format) {
            &get_sqlite_db()->copydbfile($export_filename);    
        } elsif ('csv' eq $export_format) {
            NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::copy_table(\&get_csv_db);
            &get_csv_db()->copytablefile(NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::gettablename(),$export_filename);
        } else {
            push(@$messages,'invalid extension for output filename $export_filename');
        }
    };
    if ($err or !$result) {
        push(@$messages,"loading registrations INCOMPLETE$stats");
    } else {
        push(@$messages,"loading registrations completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    destroy_stores();
    return $result;

}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
