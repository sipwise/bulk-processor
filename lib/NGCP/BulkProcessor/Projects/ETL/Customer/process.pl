use strict;

## no critic

BEGIN {
our $VERSION = '0.0';
}

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::ETL::Customer::Settings qw(
    update_settings
    update_tabular_fields
    $tabular_fields_yml

    update_load_recursive
    $load_recursive_yml

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
#use NGCP::BulkProcessor::SqlConnectors::CSVDB qw(cleanupcvsdirs);
use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw(cleanupdbfiles);
#use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw(cleanupcertfiles);

use NGCP::BulkProcessor::Projects::ETL::Customer::ProjectConnectorPool qw(destroy_all_dbs);

#use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources qw();

use NGCP::BulkProcessor::Projects::ETL::Customer::ExportCustomers qw(
    export_customers_graph
    export_customers_tabular
);
#use NGCP::BulkProcessor::Projects::ETL::Customer::ImportCustomers qw(
#    import_customers_json
#);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

#my $export_customers_graph_task_opt = 'export_customers_graph';
#push(@TASK_OPTS,$export_customers_graph_task_opt);

my $export_customers_tabular_task_opt = 'export_customers_tabular';
push(@TASK_OPTS,$export_customers_tabular_task_opt);

#my $import_customers_json_task_opt = 'import_customers_json';
#push(@TASK_OPTS,$import_customers_json_task_opt);

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
    $result &= load_config($tabular_fields_yml,\&update_tabular_fields,$YAML_CONFIG_TYPE);
    $result &= load_config($load_recursive_yml,\&update_load_recursive,$YAML_CONFIG_TYPE);
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

            #} elsif (lc($export_customers_graph_task_opt) eq lc($task)) {
            #    $result &= export_customers_graph_task(\@messages) if taskinfo($export_customers_graph_task_opt,$result);
            #    $completion |= 1;
            } elsif (lc($export_customers_tabular_task_opt) eq lc($task)) {
                $result &= export_customers_tabular_task(\@messages) if taskinfo($export_customers_tabular_task_opt,$result);
                $completion |= 1;
            #} elsif (lc($import_customers_json_task_opt) eq lc($task)) {
            #    if (taskinfo($import_customers_json_task_opt,$result,1)) {
            #        next unless check_dry();
            #        $result &= import_customers_json_task(\@messages);
            #        $completion |= 1;
            #    }

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

sub export_customers_graph_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = export_customers_graph();
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        #$stats .= "\n  total mta subscriber records: " .
        #    NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_ccacsn() . ' rows';
        #my $added_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::added_delta
        #);
        #$stats .= "\n    new: $added_count rows";
        #my $existing_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::updated_delta
        #);
        #$stats .= "\n    existing: $existing_count rows";
        #my $deleted_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::deleted_delta
        #);
        #$stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"exporting customers (graph) INCOMPLETE$stats");
    } else {
        push(@$messages,"exporting customers (graph) completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub export_customers_tabular_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = export_customers_tabular();
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        #$stats .= "\n  total mta subscriber records: " .
        #    NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_ccacsn() . ' rows';
        #my $added_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::added_delta
        #);
        #$stats .= "\n    new: $added_count rows";
        #my $existing_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::updated_delta
        #);
        #$stats .= "\n    existing: $existing_count rows";
        #my $deleted_count = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::countby_delta(
        #    $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber::deleted_delta
        #);
        #$stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"exporting customers (tabular) INCOMPLETE$stats");
    } else {
        push(@$messages,"exporting customers (tabular) completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

#sub import_customers_json_task {
#
#    my ($messages) = @_;
#    my ($result,$warning_count,$contract_read_count,$subscriber_read_count,$contract_created_count,$subscriber_created_count,$contract_failed_count,$subscriber_failed_count) = (0,0,0,0,0,0,0,0);
#    eval {
#        ($result,$warning_count,$contract_read_count,$subscriber_read_count,$contract_created_count,$subscriber_created_count,$contract_failed_count,$subscriber_failed_count) = import_customers_json();
#    };
#    my $err = $@;
#    my $stats = ": $warning_count warnings";
#    eval {
#        $stats .= "\n  contracts read: " . $contract_read_count;
#        $stats .= "\n  contracts created: " . $contract_created_count;
#        $stats .= "\n  contracts failed: " . $contract_failed_count;
#        $stats .= "\n  subscribers read: " . $subscriber_read_count;
#        $stats .= "\n  subscribers created: " . $subscriber_created_count;
#        $stats .= "\n  subscribers failed: " . $subscriber_failed_count;
#    };
#    if ($err or !$result) {
#        push(@$messages,"importing customers (json) INCOMPLETE$stats");
#    } else {
#        push(@$messages,"importing customers (json) completed$stats");
#    }
#    destroy_all_dbs(); #every task should leave with closed connections.
#    return $result;
#
#}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
