use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use Globals qw();
use Projects::Migration::IPGallery::Settings qw(
    $defaultsettings
    $defaultconfig
    update_settings
    check_dry
    $run_id
    $features_define_filename
    $dry
    $force
);
use Logging qw(
    init_log
    getlogger
    $attachmentlogfile
    scriptinfo
);
use LogError qw (
    completion
    success
    scriptwarn
    scripterror
);
use LoadConfig qw(
    load_config
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
);
use Array qw(removeduplicates);
use Utils qw(getscriptpath prompt);
use Mail qw(wrap_mailbody
		    $signature
		    $normalpriority
		    $lowpriority
		    $highpriority);

use ConnectorPool qw();
use Projects::Migration::IPGallery::ProjectConnectorPool qw();

use Projects::Migration::IPGallery::Import qw(
    import_features_define
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];
my $import_features_define_task_opt = 'import_features_define';
push(@TASK_OPTS,$import_features_define_task_opt);

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
    my $result = 0;
    my $completion = 0;

    if ('ARRAY' eq ref $tasks and (scalar @$tasks) > 0) {
        foreach my $task (@$tasks) {
            if (lc($import_features_define_task_opt) eq lc($task)) {
                scriptinfo('task: ' . $import_features_define_task_opt,getlogger(getscriptpath()));
                $result |= import_features_define_task(\@messages);
            } elsif (lc('blah') eq lc($task)) {
                scriptinfo('task: ' . 'balh',getlogger(getscriptpath()));
                next unless check_dry();
                $result |= import_features_define_task(\@messages);
                $completion |= 1;
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
    if ($result) {
        if ($completion) {
            completion(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
        } else {
            success(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
        }
    } else {
        success(join("\n\n",@messages),\@attachmentfiles,getlogger(getscriptpath()));
    }

    return $result;
}

sub cleanup_task {

}

sub import_features_define_task {

    my ($messages) = shift;
    if (import_features_define(
            $features_define_filename
        )) {
        push(@$messages,'sucessfully inserted x records...');
        return 1;
    } else {
        push(@$messages,'was not executed');
        return 0;
    }

}

END {
    # this should not be required explicitly, but prevents Log4Perl's
    # "rootlogger not initialized error upon exit..
    Projects::Migration::IPGallery::ProjectConnectorPool::destroy_dbs();
    ConnectorPool::destroy_dbs();
}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
