use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Massive::Generator::Settings qw(
    update_settings
    update_provider_config
    check_dry
    $output_path
    $defaultsettings
    $defaultconfig
    $dry
    $skip_errors
    $force

    @provider_config
    @providers
    $providers_yml

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
use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw(cleanupcertfiles);

use NGCP::BulkProcessor::ConnectorPool qw(destroy_dbs);

#use NGCP::BulkProcessor::Projects::Massive::Generator::Dao::Blah qw();

#use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();

use NGCP::BulkProcessor::Projects::Massive::Generator::Provisioning qw(
    provision_subscribers
);
use NGCP::BulkProcessor::Projects::Massive::Generator::Api qw(
    setup_provider
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $setup_provider_task_opt = 'setup_provider';
push(@TASK_OPTS,$setup_provider_task_opt);

my $provision_subscriber_task_opt = 'provision_subscriber';
push(@TASK_OPTS,$provision_subscriber_task_opt);

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
        #"run=s" => \$run_id,
        "dry" => \$dry,
        "skip-errors" => \$skip_errors,
        "force" => \$force,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($providers_yml,\&update_provider_config,$YAML_CONFIG_TYPE);

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

            } elsif (lc($setup_provider_task_opt) eq lc($task)) {
                if (taskinfo($setup_provider_task_opt,$result,1)) {
                    next unless check_dry();
                    $result &= setup_provider_task(\@messages);
                    $completion |= 1;
                }


            } elsif (lc($provision_subscriber_task_opt) eq lc($task)) {
                if (taskinfo($provision_subscriber_task_opt,$result,1)) {
                    next unless check_dry();
                    $result &= provision_subscriber_task(\@messages);
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
            cleanupcertfiles();
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

sub setup_provider_task {
    my ($messages) = @_;
    my $result = 1;
    foreach my $params (@provider_config) {
        my $provider = eval { setup_provider(%$params); };
        if ($@ or not defined $provider) {
            $result = 0;
            last unless $skip_errors;
        } else {
            my %pp = (%$params,%$provider);
            push(@providers,\%pp);
        }
    }
    my $stats = ": " . (scalar @providers) . ' resellers';
    #eval {
    #
    #};
    unless ($result) {
        push(@$messages,"setup providers INCOMPLETE$stats");
    } else {
        push(@$messages,"setup providers completed$stats");
    }
    #destroy_dbs();
    return $result;
}

sub provision_subscriber_task {

    my ($messages) = @_;
    my ($result) = (0);
    eval {
        ($result) = provision_subscribers();
    };
    my $err = $@;
    my $stats = ":";
    eval {
        $stats .= "\n  total contracts: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_status_resellerid(undef,undef) . ' rows';
        $stats .= "\n  total subscribers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(undef,undef) . ' rows';

        $stats .= "\n  total aliases: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::countby_subscriberidisprimary(undef,undef) . ' rows';
        $stats .= "\n  primary aliases: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::countby_subscriberidisprimary(undef,1) . ' rows';

        #$stats .= "\n  call forwards: " .
        #    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,undef) . ' rows';
        #
        #$stats .= "\n  registrations: " .
        #    NGCP::BulkProcessor::Dao::Trunk::kamailio::location::countby_usernamedomain(undef,undef) . ' rows';
        #
        #$stats .= "\n  trusted sources: " .
        #    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources::countby_subscriberid(undef) . ' rows';
        #
        #$stats .= "\n  non-unique contacts skipped:\n    " . join("\n    ",keys %$nonunique_contacts)
        #        if (scalar keys %$nonunique_contacts) > 0;
    };
    if ($err or !$result) {
        push(@$messages,"provision subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"provision subscribers completed$stats");
    }
    destroy_dbs();
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
