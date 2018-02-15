use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Disaster::Balances::Settings qw(
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

use NGCP::BulkProcessor::Dao::mr38::billing::contracts qw();
use NGCP::BulkProcessor::Dao::mr38::billing::contract_balances qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances qw();

use NGCP::BulkProcessor::Projects::Disaster::Balances::Check qw(
    check_fix_contract_balance_gaps_tables
    check_fix_free_cash_tables
);
#check_rest_get_items

use NGCP::BulkProcessor::Projects::Disaster::Balances::Contracts qw(
    fix_contract_balance_gaps
    fix_free_cash
);

#use NGCP::BulkProcessor::Projects::Disaster::Balances::Api qw(
#    set_call_forwards
#    set_call_forwards_batch
#);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $check_fix_contract_balance_gaps_task_opt = 'check_fix_gaps';
push(@TASK_OPTS,$check_fix_contract_balance_gaps_task_opt);

my $check_fix_free_cash_task_opt = 'check_fix_free_cash';
push(@TASK_OPTS,$check_fix_free_cash_task_opt);

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $fix_contract_balance_gaps_task_opt = 'fix_gaps';
push(@TASK_OPTS,$fix_contract_balance_gaps_task_opt);

my $fix_free_cash_task_opt = 'fix_free_cash';
push(@TASK_OPTS,$fix_free_cash_task_opt);

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

            if (lc($check_fix_contract_balance_gaps_task_opt) eq lc($task)) {
                $result &= check_fix_contract_balance_gaps_task(\@messages) if taskinfo($check_fix_contract_balance_gaps_task_opt,$result);
            } elsif (lc($check_fix_free_cash_task_opt) eq lc($task)) {
                $result &= check_fix_free_cash_task(\@messages) if taskinfo($check_fix_free_cash_task_opt,$result);
            } elsif (lc($cleanup_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages) if taskinfo($cleanup_task_opt,$result);

            } elsif (lc($fix_contract_balance_gaps_task_opt) eq lc($task)) {
                if (taskinfo($fix_contract_balance_gaps_task_opt,$result)) {
                    next unless check_dry();
                    $result &= fix_contract_balance_gaps_task(\@messages);
                    $completion |= 1;
                }

            } elsif (lc($fix_free_cash_task_opt) eq lc($task)) {
                if (taskinfo($fix_free_cash_task_opt,$result)) {
                    next unless check_dry();
                    $result &= fix_free_cash_task(\@messages);
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

sub check_fix_contract_balance_gaps_task {
    my ($messages) = @_;
    my @check_messages = ();
    my $result = check_fix_contract_balance_gaps_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    #@check_messages = ();
    #$result = check_provisioning_db_tables(\@check_messages);
    ##$result &= ..
    #push(@$messages,join("\n",@check_messages));


    destroy_dbs();
    return $result;
}

sub check_fix_free_cash_task {
    my ($messages) = @_;
    my @check_messages = ();
    my $result = check_fix_free_cash_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    #@check_messages = ();
    #$result = check_provisioning_db_tables(\@check_messages);
    ##$result &= ..
    #push(@$messages,join("\n",@check_messages));


    destroy_dbs();
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



sub fix_contract_balance_gaps_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = fix_contract_balance_gaps();
        #if ($batch) {
        #    ($result,$warning_count) = set_barring_profiles_batch();
        #} else {
        #    ($result,$warning_count) = set_barring_profiles();
        #}
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {

    };
    if ($err or !$result) {
        push(@$messages,"fix contract balances gaps INCOMPLETE$stats");
    } else {
        push(@$messages,"fix contract balances gaps completed$stats");
    }
    destroy_dbs(); #every task should leave with closed connections.
    return $result;

}


sub fix_free_cash_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = fix_free_cash();
        #if ($batch) {
        #    ($result,$warning_count) = set_barring_profiles_batch();
        #} else {
        #    ($result,$warning_count) = set_barring_profiles();
        #}
    };
    my $err = $@;
    my $stats = ($skip_errors ? ": $warning_count warnings" : '');
    eval {

    };
    if ($err or !$result) {
        push(@$messages,"fix free cash INCOMPLETE$stats");
    } else {
        push(@$messages,"fix free cash completed$stats");
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
