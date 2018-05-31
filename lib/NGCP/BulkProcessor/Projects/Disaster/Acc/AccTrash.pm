package NGCP::BulkProcessor::Projects::Disaster::Acc::AccTrash;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
use DateTime qw();
use Time::HiRes qw(sleep);

use NGCP::BulkProcessor::Projects::Disaster::Acc::Settings qw(
    $dry
    $skip_errors

    $process_acc_trash_multithreading
    $process_acc_trash_numofthreads
    $process_acc_trash_blocksize

    $delete_cdr

    $sleep_secs
    $acc_record_limit
);
#$set_preference_bulk_numofthreads

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();
use NGCP::BulkProcessor::Dao::Trunk::kamailio::acc qw();
use NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash qw();



use NGCP::BulkProcessor::ConnectorPool qw(

    get_kamailio_db
    get_accounting_db
);
#get_xa_db

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    process_acc_trash
);

sub process_acc_trash {

    my ($process_acc_trash,$fix_lnp_prefix_tokens) = @_;
    my $static_context = {
        delete_cdr => $delete_cdr,
        sleep_secs => $sleep_secs,
        acc_record_limit => $acc_record_limit,
        process_acc_trash => $process_acc_trash,
        fix_lnp_prefix_tokens => $fix_lnp_prefix_tokens,
    };
    my $result = _process_acc_trash_checks($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            if ($context->{sleep_secs} > 0.0 and $context->{acc_record_limit} > 0) {
                my $sleep = $context->{sleep_secs};
                while (((my $acc_count = NGCP::BulkProcessor::Dao::Trunk::kamailio::acc::count($context->{acc_db})) + scalar @$records) > $context->{acc_record_limit}) {
                    _info($context,"$acc_count acc records (limit $context->{acc_record_limit}), sleep for $sleep secs ...");
                    sleep($sleep);
                    $sleep *= 2.0 if $sleep < 30.0; #manchester
                }
            }
            foreach my $acc_trash (@$records) {
                $rownum++;
                next unless _reset_process_acc_trash_context($context,$acc_trash,$rownum);
                #if (_delete_cdr($context)) {
                #    _process_acc_trash($context);
                #}
                _delete_cdr($context) if $context->{delete_cdr};
                _process_acc_trash($context) if $context->{process_acc_trash};
            }

            #return 0;
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{acc_db} = &get_kamailio_db();
            $context->{cdr_db} = &get_accounting_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            # below is not mandatory..
            _check_insert_tables();
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{acc_db};
            undef $context->{cdr_db};
            destroy_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 0,
        multithreading => $process_acc_trash_multithreading,
        numofthreads => $process_acc_trash_numofthreads,
        blocksize => $process_acc_trash_blocksize,
    ),$warning_count);
}


sub _check_insert_tables {

    #NGCP::BulkProcessor::Dao::mr38::provisioning::voip_usr_preferences::check_table();

}

sub _process_acc_trash {
    my ($context) = @_;

    eval {
        $context->{acc_db}->db_begin();

        my $incomplete = 0;
        foreach my $acc_trash (@{$context->{acc_trash}}) {
            my $src_leg = $acc_trash->{src_leg};
            #my $move = 1;
            if ($context->{fix_lnp_prefix_tokens}) {
                if (defined $src_leg and length($src_leg) > 0) {
                    my @tokens = split(/\|/,$src_leg,-1);
                    my $tokencount = scalar @tokens;
                    if ($tokencount == 24) {
                        $acc_trash->{src_leg} .= '|';
                    } elsif ($tokencount == 25) {
                        _info($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " seems to be correct (lnp_prefix), src_leg token count = $tokencount");
                    } elsif ($tokencount > 0) {
                        if ($skip_errors) {
                            _warn($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " cannot be fixed (lnp_prefix), src_leg token count = $tokencount");
                        } else {
                            _error($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " cannot be fixed (lnp_prefix), src_leg token count = $tokencount");
                        }
                        $incomplete = 1;
                        #$move = 0;
                        #last;
                    }
                }
                my $dst_leg = $acc_trash->{dst_leg};
                if (defined $dst_leg and length($dst_leg) > 0) {
                    my @tokens = split(/\|/,$dst_leg,-1);
                    my $tokencount = scalar @tokens;
                    if ($tokencount == 22) {
                        $acc_trash->{dst_leg} .= '|';
                    } elsif ($tokencount == 23) {
                        _info($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " seems to be correct (lnp_prefix), dst_leg token count = $tokencount");
                    } elsif ($tokencount > 0) {
                        if ($skip_errors) {
                            _warn($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " cannot be fixed (lnp_prefix), dst_leg token count = $tokencount");
                        } else {
                            _error($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " cannot be fixed (lnp_prefix), dst_leg token count = $tokencount");
                        }
                        $incomplete = 1;
                        #$move = 0;
                        #last;
                    }
                }
            }
            unless ($incomplete) {
                NGCP::BulkProcessor::Dao::Trunk::kamailio::acc::insert_row($context->{acc_db},$acc_trash);
                _info($context,"($context->{rownum}) " . 'acc trash record id ' . $acc_trash->{id} . " copied",1);
            }
        }
        unless ($incomplete) {
            NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash::delete_callids($context->{acc_db},[ $context->{call_id} ]);
            _info($context,"($context->{rownum}) " . 'acc trash records for callid ' . $context->{call_id} . " deleted",1);
        }

        if ($dry or $incomplete) {
            $context->{acc_db}->db_rollback(0);
        } else {
            $context->{acc_db}->db_commit();
        }

    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{acc_db}->db_rollback(1);
        };
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'kamailio database error with acc trash callid ' . $context->{call_id} . ': ' . $err);
        } else {
            _error($context,"($context->{rownum}) " . 'kamailio database error with acc trash callid ' . $context->{call_id} . ': ' . $err);
        }
    }
}

sub _delete_cdr {
    my ($context) = @_;

    eval {
        $context->{cdr_db}->db_begin();

        if (NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::delete_callids($context->{cdr_db},[ $context->{call_id} ])) {
            _info($context,'cdr id ' . $context->{call_id} . " deleted");
        } else {
            _info($context,'no cdr id ' . $context->{call_id} . " to delete",1);
        }

        if ($dry) {
            $context->{cdr_db}->db_rollback(0);
        } else {
            $context->{cdr_db}->db_commit();
        }

    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{cdr_db}->db_rollback(1);
        };
        if ($skip_errors) {
            _warn($context,"($context->{rownum}) " . 'accounting database error with acc trash callid ' . $context->{call_id} . ': ' . $err);
        } else {
            _error($context,"($context->{rownum}) " . 'accounting database error with acc trash callid ' . $context->{call_id} . ': ' . $err);
        }
        #return 0;
    }
    #return 1;

}

sub _process_acc_trash_checks {
    my ($context) = @_;

    my $result = _checks($context);

    return $result;
}

sub _reset_process_acc_trash_context {

    my ($context,$acc_trash,$rownum) = @_;

    my $result = _reset_context($context,$acc_trash,$rownum);

    $context->{call_id} = $acc_trash->{callid};
    $context->{acc_trash} = NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash::findby_callids($context->{acc_db},[ $context->{call_id} ]);

    ##$context->{barring_profile} = $imported_subscriber->{barring_profile};
    ##$context->{ncos_level} = $context->{ncos_level_map}->{$context->{barring_profile}};

    ##delete $context->{adm_ncos_id_preference_id};

    return $result;

}


sub _checks  {

    my ($context) = @_;

    my $result = 1;
    my $acctrashcount = 0;
    eval {
        $acctrashcount = NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash::count();
    };
    if ($@ or $acctrashcount == 0) {
        rowprocessingerror(threadid(),'no acc trash records',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    #my $userpasswordcount = 0;
    #eval {
    #    $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn();
    #};
    #if ($@ or $userpasswordcount == 0) {
    #    rowprocessingerror(threadid(),'please import user passwords first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}
    #my $subscribercount = 0;
    #my $subscriber_barring_profiles = [];
    #eval {
    #    $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
    #    $subscriber_barring_profiles = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::list_barringprofiles();
    #};
    #if ($@ or $subscribercount == 0) {
    #    rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}

    return $result;

}

sub _reset_context {

    my ($context,$acc_trash,$rownum) = @_;

    my $result = 1;

    $context->{rownum} = $rownum;

    undef $context->{call_id};
    undef $context->{acc_trash}; # = $acc_trash;

    return $result;

}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;
