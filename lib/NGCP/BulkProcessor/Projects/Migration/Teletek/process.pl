use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use Getopt::Long qw(GetOptions);
use Fcntl qw(LOCK_EX LOCK_NB);

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Settings qw(
    update_settings
    update_reseller_mapping
    update_barring_profiles
    check_dry
    $output_path
    $defaultsettings
    $defaultconfig
    $dry
    $skip_errors
    $force
    $run_id
    @subscriber_filenames
    $reseller_mapping_yml
    $barring_profiles_yml

    @allowedcli_filenames

    @clir_filenames

    @callforward_filenames

    @registration_filenames
);
#$allowed_ips

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

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(destroy_all_dbs);

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources qw();

use NGCP::BulkProcessor::Dao::Trunk::kamailio::location qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Check qw(
    check_billing_db_tables
    check_provisioning_db_tables
    check_kamailio_db_tables
    check_import_db_tables
    check_rest_get_items
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::Import qw(
    import_subscriber
    import_allowedcli
    import_clir
    import_callforward
    import_registration
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::Provisioning qw(
    provision_subscribers
);

scripterror(getscriptpath() . ' already running',getlogger(getscriptpath())) unless flock DATA, LOCK_EX | LOCK_NB; # not tested on windows yet

my @TASK_OPTS = ();

my $tasks = [];

my $check_task_opt = 'check';
push(@TASK_OPTS,$check_task_opt);

my $cleanup_task_opt = 'cleanup';
push(@TASK_OPTS,$cleanup_task_opt);
my $cleanup_all_task_opt = 'cleanup_all';
push(@TASK_OPTS,$cleanup_all_task_opt);

my $import_subscriber_task_opt = 'import_subscriber';
push(@TASK_OPTS,$import_subscriber_task_opt);
my $import_truncate_subscriber_task_opt = 'truncate_subscriber';
push(@TASK_OPTS,$import_truncate_subscriber_task_opt);

my $import_allowedcli_task_opt = 'import_allowedcli';
push(@TASK_OPTS,$import_allowedcli_task_opt);
my $import_truncate_allowedcli_task_opt = 'truncate_allowedcli';
push(@TASK_OPTS,$import_truncate_allowedcli_task_opt);

my $import_clir_task_opt = 'import_clir';
push(@TASK_OPTS,$import_clir_task_opt);
my $import_truncate_clir_task_opt = 'truncate_clir';
push(@TASK_OPTS,$import_truncate_clir_task_opt);

my $import_callforward_task_opt = 'import_callforward';
push(@TASK_OPTS,$import_callforward_task_opt);
my $import_truncate_callforward_task_opt = 'truncate_callforward';
push(@TASK_OPTS,$import_truncate_callforward_task_opt);

my $import_registration_task_opt = 'import_registration';
push(@TASK_OPTS,$import_registration_task_opt);
my $import_truncate_registration_task_opt = 'truncate_registration';
push(@TASK_OPTS,$import_truncate_registration_task_opt);

my $create_subscriber_task_opt = 'create_subscriber';
push(@TASK_OPTS,$create_subscriber_task_opt);

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
        "run=s" => \$run_id,
        "dry" => \$dry,
        "skip-errors" => \$skip_errors,
        "force" => \$force,
    ); # or scripterror('error in command line arguments',getlogger(getscriptpath()));

    $tasks = removeduplicates($tasks,1);

    my $result = load_config($configfile);
    init_log();
    $result &= load_config($settingsfile,\&update_settings,$SIMPLE_CONFIG_TYPE);
    $result &= load_config($reseller_mapping_yml,\&update_reseller_mapping,$YAML_CONFIG_TYPE);
    $result &= load_config($barring_profiles_yml,\&update_barring_profiles,$YAML_CONFIG_TYPE);
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

            if (lc($check_task_opt) eq lc($task)) {
                $result &= check_task(\@messages) if taskinfo($check_task_opt,$result);

            } elsif (lc($cleanup_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,0) if taskinfo($cleanup_task_opt,$result);
            } elsif (lc($cleanup_all_task_opt) eq lc($task)) {
                $result &= cleanup_task(\@messages,1) if taskinfo($cleanup_all_task_opt,$result);

            } elsif (lc($import_subscriber_task_opt) eq lc($task)) {
                $result &= import_subscriber_task(\@messages) if taskinfo($import_subscriber_task_opt,$result);
            } elsif (lc($import_truncate_subscriber_task_opt) eq lc($task)) {
                $result &= import_truncate_subscriber_task(\@messages) if taskinfo($import_truncate_subscriber_task_opt,$result);


            } elsif (lc($import_allowedcli_task_opt) eq lc($task)) {
                $result &= import_allowedcli_task(\@messages) if taskinfo($import_allowedcli_task_opt,$result);
            } elsif (lc($import_truncate_allowedcli_task_opt) eq lc($task)) {
                $result &= import_truncate_allowedcli_task(\@messages) if taskinfo($import_truncate_allowedcli_task_opt,$result);

            } elsif (lc($import_clir_task_opt) eq lc($task)) {
                $result &= import_clir_task(\@messages) if taskinfo($import_clir_task_opt,$result);
            } elsif (lc($import_truncate_clir_task_opt) eq lc($task)) {
                $result &= import_truncate_clir_task(\@messages) if taskinfo($import_truncate_clir_task_opt,$result);

            } elsif (lc($import_callforward_task_opt) eq lc($task)) {
                $result &= import_callforward_task(\@messages) if taskinfo($import_callforward_task_opt,$result);
            } elsif (lc($import_truncate_callforward_task_opt) eq lc($task)) {
                $result &= import_truncate_callforward_task(\@messages) if taskinfo($import_truncate_callforward_task_opt,$result);

            } elsif (lc($import_registration_task_opt) eq lc($task)) {
                $result &= import_registration_task(\@messages) if taskinfo($import_registration_task_opt,$result);
            } elsif (lc($import_truncate_registration_task_opt) eq lc($task)) {
                $result &= import_truncate_registration_task(\@messages) if taskinfo($import_truncate_registration_task_opt,$result);

            } elsif (lc($create_subscriber_task_opt) eq lc($task)) {
                if (taskinfo($create_subscriber_task_opt,$result,1)) {
                    next unless check_dry();
                    $result &= create_subscriber_task(\@messages);
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

sub check_task {
    my ($messages) = @_;
    my @check_messages = ();
    my $result = check_billing_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_provisioning_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_kamailio_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_rest_get_items(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));

    @check_messages = ();
    $result = check_import_db_tables(\@check_messages);
    #$result &= ..
    push(@$messages,join("\n",@check_messages));


    destroy_all_dbs();
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

sub import_subscriber_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_subscriber(@subscriber_filenames);
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total subscriber records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_ccacsn() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"importing subscribers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_subscriber_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total subscriber records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_ccacsn() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported subscribers completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_allowedcli_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_allowedcli(@allowedcli_filenames);
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total allowed cli records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_ccacsn() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing allowed clis (additional numbers) INCOMPLETE$stats");
    } else {
        push(@$messages,"importing allowed clis (additional numbers) completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub import_truncate_allowedcli_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total allowed cli (additional numbers) records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_ccacsn() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported allowed clis (additional numbers) INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported allowed clis (additional numbers) completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_clir_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_clir(@clir_filenames);
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total clir records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_clir() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing clir INCOMPLETE$stats");
    } else {
        push(@$messages,"importing clir completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_truncate_clir_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total clir records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_clir() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported clir INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported clir completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_callforward_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_callforward(@callforward_filenames);
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total callforward records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_ccacsntype() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing callforwards INCOMPLETE$stats");
    } else {
        push(@$messages,"importing callforwards completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_truncate_callforward_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total callforward records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_ccacsntype() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported callforwards INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported callforwards completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_registration_task {

    my ($messages) = @_;
    my ($result,$warning_count) = (0,0);
    eval {
        ($result,$warning_count) = import_registration(@registration_filenames);
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total registration records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_sipcontact() . ' rows';
        my $added_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::added_delta
        );
        $stats .= "\n    new: $added_count rows";
        my $existing_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::updated_delta
        );
        $stats .= "\n    existing: $existing_count rows";
        my $deleted_count = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_delta(
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::deleted_delta
        );
        $stats .= "\n    removed: $deleted_count rows";
    };
    if ($err or !$result) {
        push(@$messages,"importing registrations INCOMPLETE$stats");
    } else {
        push(@$messages,"importing registrations completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}


sub import_truncate_registration_task {

    my ($messages) = @_;
    my $result = 0;
    eval {
        $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::create_table(1);
    };
    my $err = $@;
    my $stats = '';
    eval {
        $stats .= "\n  total registration records: " .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_sipcontact() . ' rows';
    };
    if ($err or !$result) {
        push(@$messages,"truncating imported registrations INCOMPLETE$stats");
    } else {
        push(@$messages,"truncating imported registrations completed$stats");
    }
    destroy_all_dbs(); #every task should leave with closed connections.
    return $result;

}

sub create_subscriber_task {

    my ($messages) = @_;
    my ($result,$warning_count,$nonunique_contacts) = (0,0,{});
    eval {
        ($result,$warning_count,$nonunique_contacts) = provision_subscribers();
    };
    my $err = $@;
    my $stats = ": $warning_count warnings";
    eval {
        $stats .= "\n  total contracts: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::contracts::countby_status_resellerid(undef,undef) . ' rows';
        $stats .= "\n  total subscribers: " .
            NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(undef,undef) . ' rows';

        $stats .= "\n  total aliases: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::countby_subscriberidisprimary(undef,undef) . ' rows';
        $stats .= "\n  primary aliases: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::countby_subscriberidisprimary(undef,1) . ' rows';

        $stats .= "\n  call forwards: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::countby_subscriberid_type(undef,undef) . ' rows';

        $stats .= "\n  registrations: " .
            NGCP::BulkProcessor::Dao::Trunk::kamailio::location::countby_usernamedomain(undef,undef) . ' rows';

        $stats .= "\n  trusted sources: " .
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources::countby_subscriberid(undef) . ' rows';

        $stats .= "\n  non-unique contacts skipped:\n    " . join("\n    ",keys %$nonunique_contacts)
                if (scalar keys %$nonunique_contacts) > 0;
    };
    if ($err or !$result) {
        push(@$messages,"create subscribers INCOMPLETE$stats");
    } else {
        push(@$messages,"create subscribers completed$stats");
        if (not $dry) {
            push(@$messages,"YOU MIGHT WANT TO RESTART KAMAILIO FOR PERMANENT REGISTRATIONS TO COME INTO EFFECT");
        }
    }
    destroy_all_dbs();
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