use strict;

## no critic

our $VERSION = "0.0";

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::ETL::CDR::Settings qw(
    update_settings
    update_tabular_fields
    update_graph_fields
    $tabular_yml
    $graph_yml

    update_load_recursive
    get_export_filename
    $cdr_export_filename_format
    $load_yml

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

use NGCP::BulkProcessor::Projects::ETL::CDR::ProjectConnectorPool qw(destroy_all_dbs get_csv_db get_sqlite_db);

use NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular qw();

use NGCP::BulkProcessor::Projects::ETL::CDR::ExportCDR qw(
    export_cdr_graph
    export_cdr_tabular
);
#use NGCP::BulkProcessor::Projects::ETL::Cdr::ImportCdr qw(
#    import_cdr_json
#);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB;

my @TASK_OPTS = ();

my $tasks = [];

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);

my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $export_cdr_graph_task_opt = 'export_cdr_graph';
push(@TASK_OPTS,$export_cdr_graph_task_opt);

my $export_cdr_tabular_task_opt = 'export_cdr_tabular';
push(@TASK_OPTS,$export_cdr_tabular_task_opt);

#my $import_cdr_json_task_opt = 'import_cdr_json';
#push(@TASK_OPTS,$import_cdr_json_task_opt);

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
    );

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($tabular_yml,\&update_tabular_fields,$YAML_CONFIG_TYPE);
    $result &= load_config($graph_yml,\&update_graph_fields,$YAML_CONFIG_TYPE);
    $result &= load_config($load_yml,\&update_load_recursive,$YAML_CONFIG_TYPE);
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

            } elsif (lc($export_cdr_graph_task_opt) eq lc($task)) {
                $result &= export_cdr_graph_task(\@messages) if taskinfo($export_cdr_graph_task_opt,$result);
                $completion |= 1;
            } elsif (lc($export_cdr_tabular_task_opt) eq lc($task)) {
                $result &= export_cdr_tabular_task(\@messages) if taskinfo($export_cdr_tabular_task_opt,$result);
                $completion |= 1;
            #} elsif (lc($import_cdr_json_task_opt) eq lc($task)) {
            #    if (taskinfo($import_cdr_json_task_opt,$result,1)) {
            #        next unless check_dry();
            #        $result &= import_cdr_json_task(\@messages);
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

sub export_cdr_graph_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = export_cdr_graph();
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
        push(@$messages,"exporting cdr (graph) INCOMPLETE$stats");
    } else {
        push(@$messages,"exporting cdr (graph) completed$stats");
    }
    destroy_all_dbs(); 
    return $result;

}

sub export_cdr_tabular_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = export_cdr_tabular();
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total subscriber records: " .
            NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::countby_delta() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::countby_delta(
            $NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
        my ($export_filename,$export_format) = get_export_filename($cdr_export_filename_format);
        if ('sqlite' eq $export_format) {
            &get_sqlite_db()->copydbfile($export_filename);    
        } elsif ('csv' eq $export_format) {
            NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::copy_table(\&get_csv_db);
            &get_csv_db()->copytablefile(NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::gettablename(),$export_filename);
        } else {
            push(@$messages,'invalid extension for output filename $export_filename');
        }
    };
    if ($err or !$result) {
        push(@$messages,"exporting cdr (tabular) INCOMPLETE$stats");
    } else {
        push(@$messages,"exporting cdr (tabular) completed$stats");
    }
    destroy_all_dbs(); 
    return $result;

}

#sub import_cdr_json_task {
#
#    my ($messages) = @_;
#    my ($result,$warning_count,$contract_read_count,$subscriber_read_count,$contract_created_count,$subscriber_created_count,$contract_failed_count,$subscriber_failed_count) = (0,0,0,0,0,0,0,0);
#    eval {
#        ($result,$warning_count,$contract_read_count,$subscriber_read_count,$contract_created_count,$subscriber_created_count,$contract_failed_count,$subscriber_failed_count) = import_cdr_json();
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
#        push(@$messages,"importing cdr (json) INCOMPLETE$stats");
#    } else {
#        push(@$messages,"importing cdr (json) completed$stats");
#    }
#    destroy_all_dbs();
#    return $result;
#
#}

__DATA__
This exists to allow the locking code at the beginning of the file to work.
DO NOT REMOVE THESE LINES!
