package NGCP::BulkProcessor::Projects::Export::Ama::Ccs::CDR;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Ccs::Settings qw(

    $skip_errors

    $export_cdr_multithreading
    $export_cdr_numofthreads
    $export_cdr_blocksize
    $export_cdr_joins
    $export_cdr_conditions
    $export_cdr_limit
    $export_cdr_stream
    $export_cdr_rollover_fsn

    $ama_sensor_id
    $ama_recording_office_id
    $ama_incoming_trunk_group_number
    $ama_outgoing_trunk_group_number
    $ama_originating_digits_cdr_field
    $ama_terminating_digits_cdr_field

    $ivr_duration_limit
    $primary_alias_pattern

    $switch_number_pattern
    $switch_number_replacement

    $originating_pattern
    $originating_replacement

    $terminating_pattern
    $terminating_replacement

    $terminating_open_digits_6001
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
    fileerror
);

use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_group qw();
use NGCP::BulkProcessor::Dao::Trunk::accounting::mark qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::File qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Record qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module000 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module104 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module199 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module611 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::NetworkOperatorData qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
    ping_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid kbytes2gigs); # stringtobool check_ipnet trim);

use NGCP::BulkProcessor::Calendar qw(from_epoch);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    export_cdrs
    reset_fsn
    reset_export_status
);

my $BLIND_TRANSFER_NO_IVR = 1;
my $BLIND_TRANSFER = 2;
my $NO_TRANSFER_NO_IVR = 3;
my $NO_TRANSFER = 4;
my $ATTN_TRANSFER_NO_IVR = 5;
my $ATTN_TRANSFER = 6;
my $CFU = 7;

my $file_sequence_number : shared = 0;
my $rowcount : shared = 0;

sub reset_export_status {

    my ($from,$to) = @_;

    my $result = 1;
    my $context = { tid => threadid(), warning_count => 0, error_count => 0, };
    $result &= _check_export_status_stream($context);
    my $updated;
    eval {
        $updated = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::update_export_status($context->{export_status_id},
            $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::UNEXPORTED,$from,$to);
    };
    if ($@) {
        if ($skip_errors) {
            _warn($context,"problem with export status reset: " . $@);
        } else {
            _error($context,"problem with export status reset: " . $@);
        }
        $result = 0;
    } else {
        _info($context,"$updated export states reset");
    }

    return $result;

}

sub reset_fsn {

    my $result = 1;
    my $context = { tid => threadid(), warning_count => 0, error_count => 0, };
    $result &= _check_export_status_stream($context);
    #my $fsn;
    eval {
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::delete_system_marks(undef,
            $export_cdr_stream,
        );
        #NGCP::BulkProcessor::Dao::Trunk::accounting::mark::cleanup_system_marks(undef,
        #    $export_cdr_stream,
        #);
        #NGCP::BulkProcessor::Dao::Trunk::accounting::mark::set_system_mark(undef,
        #    $export_cdr_stream,
        #    '0' #$NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn
        #);
        #$fsn = NGCP::BulkProcessor::Dao::Trunk::accounting::mark::get_system_mark(undef,
        #    $export_cdr_stream
        #); #load mark...
    };
    if ($@) {
        if ($skip_errors) {
            _warn($context,"problem with file sequence number reset: " . $@);
        } else {
            _error($context,"problem with file sequence number reset: " . $@);
        }
        $result = 0;
    } else {
        _info($context,"file sequence number deleted"); #reset to $fsn")
    }
    return $result;

}

sub export_cdrs {

    my $static_context = {};
    my $result = _export_cdrs_create_context($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    my @ama_files : shared = ();
    $result &= NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::process_unexported(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            # we only want to export a call scenario of eg. 4 cdrs, if the block (eg. 1000 cdrs) contains all 4.
            $context->{block_call_id_map} = {};
            my $cdr_id_map = {};
            foreach my $record (@$records) {
                my ($id,$call_id) = @$record;
                my $call_id_prefix = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::get_callidprefix($call_id);
                if (exists $context->{block_call_id_map}->{$call_id_prefix}) {
                    $context->{block_call_id_map}->{$call_id_prefix} += 1;
                } else {
                    $context->{block_call_id_map}->{$call_id_prefix} = 1;
                }
                $cdr_id_map->{$id} = 1;
            }
            $context->{correlated_cdrs_map} = {};
            foreach my $record (@$records) {
                my ($id,$call_id) = @$record;
                $context->{correlated_cdrs_map}->{$id} = _find_child_cdrs($context,$id);
                my $call_id_prefix = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::get_callidprefix($call_id);
                map { $context->{block_call_id_map}->{$call_id_prefix} += 1 if exists $cdr_id_map->{$_->{id}}; } @{$context->{correlated_cdrs_map}->{$id}};
            }
            foreach my $record (@$records) {
                if (defined $export_cdr_limit) {
                    lock $rowcount;
                    if ($rowcount >= $export_cdr_limit) {
                        _info($context,"exceeding export limit $export_cdr_limit");
                        return 0;
                    }
                }
                if ($context->{file_sequence_number} > $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn) {
                    _info($context,"exceeding file sequence number " . $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn);
                    return 0;
                }
                my ($id,$call_id) = @$record;
                next unless _export_cdrs_init_context($context,$id,$call_id);
                eval {
                    $context->{file}->write_record(
                        get_transfer_in => \&_get_transfer_in,
                        get_record => \&_get_record,
                        get_transfer_out => \&_get_transfer_out,
                        commit_cb => \&_commit_export_status,
                        context => $context,
                    );
                };
                if ($@) {
                    if ($skip_errors) {
                        _warn($context,"problem while exporting call id $call_id (cdr id $id): " . $@);
                    } else {
                        _error($context,"problem while exporting call id $call_id (cdr id $id): " . $@);
                    }
                }
            }
            ping_dbs();
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;

            $context->{ama_files} = [];
            $context->{has_next} = 1;
            #$context->{rownum} = 0;

            _increment_file_sequence_number($context);
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            $context->{has_next} = 0; #do not reserve another file sequence number
            if ($context->{file_sequence_number} <= $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn) {
                eval {
                    $context->{file}->close(
                        get_transfer_out => \&_get_transfer_out,
                        commit_cb => \&_commit_export_status,
                        context => $context,
                    );
                };
                if ($@) {
                    if ($skip_errors) {
                        _warn($context,"problem while closing " . $context->{file}->get_filename() . ": " . $@);
                    } else {
                        _error($context,"problem while closing " . $context->{file}->get_filename() . ": " . $@);
                    }
                }
            }

            undef $context->{db};
            destroy_dbs();

            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
                push(@ama_files,@{$context->{ama_files}});
            }
        },
        load_recursive => 0,
        blocksize => $export_cdr_blocksize,
        multithreading => $export_cdr_multithreading,
        numofthreads => $export_cdr_numofthreads,
        joins => $export_cdr_joins,
        conditions => $export_cdr_conditions,
        #sort => [{ column => 'id', numeric => 1, dir => 1 }],
        #limit => $export_cdr_limit,
    );

    eval {
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::cleanup_system_marks(undef,
        $export_cdr_stream);
    };
    if ($@) {
        if ($skip_errors) {
            _warn($static_context,"problem with file sequence number cleanup: " . $@);
        } else {
            _error($static_context,"problem with file sequence number cleanup: " . $@);
        }
        $result = 0;
    } else {
        _info($static_context,"file sequence numbers cleaned up");
    }

    return ($result,$warning_count,\@ama_files);
}

sub _find_child_cdrs {
    my ($context,$id) = @_;
    my @correlated_cdrs = ();
    foreach my $correlated_group_cdr (@{NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_group::findby_cdrid($context->{db},$id)}) {
        foreach my $correlated_cdr (@{NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callid($context->{db},$correlated_group_cdr->{call_id})}) {
            push(@correlated_cdrs,$correlated_cdr);
        }
    }
    return \@correlated_cdrs;
}

sub _export_cdrs_init_context {

    my ($context,$cdr_id,$call_id) = @_;

    my $result = 0;
    my $parent_cdrs;
    $context->{call_id} = $call_id;
    my $scenario = { code => 0, ama => [], };
    $context->{scenario} = $scenario;

    if (not exists $context->{file_cdr_id_map}->{$cdr_id}) { #skip if processed for a file already
        my $call_id_prefix = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::get_callidprefix($call_id);
        if (exists $context->{block_call_id_map}->{$call_id_prefix}) { #skip if this callid form parent calls that have already been processed
            if ((scalar @{NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_group::findby_callid($context->{db},$call_id)}) == 0) { #skip if this is a correlated (child) cdr
                $parent_cdrs = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callidprefix($context->{db},
                    $call_id,$export_cdr_joins,$export_cdr_conditions); #already sorted
                my @scenario_cdrs = @$parent_cdrs;
                foreach my $cdr (@$parent_cdrs) {
                    my @correlated_cdrs;
                    if (exists $context->{correlated_cdrs_map}->{$cdr->{id}}) {
                        @correlated_cdrs = @{$context->{correlated_cdrs_map}->{$cdr->{id}}};
                    } else {
                        @correlated_cdrs = @{_find_child_cdrs($context,$cdr->{id})};
                    }
                    $cdr->{_correlated_cdrs} = \@correlated_cdrs;
                    push(@scenario_cdrs,@correlated_cdrs);
                }
                $scenario->{parent_cdrs} = $parent_cdrs;
                $scenario->{all_cdrs} = \@scenario_cdrs;
                my $cdrs_in_block = delete $context->{block_call_id_map}->{$call_id_prefix};
                if ((scalar @scenario_cdrs) == $cdrs_in_block) {
                    my $malformed = 0;

                    #blind xfer:
                    if ((scalar @$parent_cdrs) == 2
                        and not $parent_cdrs->[0]->is_xfer()
                        and $parent_cdrs->[1]->is_xfer()
                        and (scalar @{$parent_cdrs->[0]->{_correlated_cdrs}}) == 0
                        and (scalar @{$parent_cdrs->[1]->{_correlated_cdrs}}) == 0
                        and ($scenario->{ccs_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$parent_cdrs->[0]->{destination_user_id}))
                        and ($scenario->{ccs_subscriber}->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($scenario->{ccs_subscriber}->{id},1)->[0])
                        and (not defined $primary_alias_pattern or $scenario->{ccs_subscriber}->{primary_alias}->{username} =~ $primary_alias_pattern)
                        ) {
                        my $ivr_duration = abs($parent_cdrs->[0]->{start_time} - $parent_cdrs->[1]->{init_time});
                        if ($ivr_duration < $ivr_duration_limit) {
                            $scenario->{code} = $BLIND_TRANSFER_NO_IVR;
                        } else {
                            $scenario->{code} = $BLIND_TRANSFER;
                        }
                        $result = 1;
                    #no transfer:
                    } elsif ((scalar @$parent_cdrs) == 1
                        and not $parent_cdrs->[0]->is_xfer()
                        and not $parent_cdrs->[0]->is_pbx()
                        and (scalar @{$parent_cdrs->[0]->{_correlated_cdrs}}) == 0
                        and ($scenario->{ccs_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$parent_cdrs->[0]->{destination_user_id}))
                        and ($scenario->{ccs_subscriber}->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($scenario->{ccs_subscriber}->{id},1)->[0])
                        and (not defined $primary_alias_pattern or $scenario->{ccs_subscriber}->{primary_alias}->{username} =~ $primary_alias_pattern)
                        ) {
                        my $ivr_duration = $parent_cdrs->[0]->{duration};
                        if ($ivr_duration < $ivr_duration_limit) {
                            $scenario->{code} = $NO_TRANSFER_NO_IVR;
                        } else {
                            $scenario->{code} = $NO_TRANSFER;
                        }
                        $result = 1;
                    #attn transfer:
                    } elsif ((scalar @$parent_cdrs) == 2
                        and not $parent_cdrs->[0]->is_pbx()
                        and $parent_cdrs->[1]->is_pbx()
                        and (scalar @{$parent_cdrs->[0]->{_correlated_cdrs}}) == 0
                        and (scalar @{$parent_cdrs->[1]->{_correlated_cdrs}}) == 1
                        and ($scenario->{ccs_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$parent_cdrs->[0]->{destination_user_id}))
                        and ($scenario->{ccs_subscriber}->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($scenario->{ccs_subscriber}->{id},1)->[0])
                        and (not defined $primary_alias_pattern or $scenario->{ccs_subscriber}->{primary_alias}->{username} =~ $primary_alias_pattern)
                        ) {
                        my $correlated_cdr = $parent_cdrs->[1]->{_correlated_cdrs}->[0];
                        my $ivr_duration = abs($correlated_cdr->{start_time} - $parent_cdrs->[1]->{init_time});
                        if ($ivr_duration < $ivr_duration_limit) {
                            $scenario->{code} = $ATTN_TRANSFER_NO_IVR;
                        } else {
                            $scenario->{code} = $ATTN_TRANSFER;
                        }
                        $result = 1;
                    #cfu:
                    } elsif ((scalar @$parent_cdrs) == 2
                        and not $parent_cdrs->[0]->is_pbx()
                        and $parent_cdrs->[1]->is_pbx()
                        and $parent_cdrs->[1]->{call_type} eq $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::CFU_CALL_TYPE
                        and (scalar @{$parent_cdrs->[0]->{_correlated_cdrs}}) == 0
                        and (scalar @{$parent_cdrs->[1]->{_correlated_cdrs}}) == 0
                        and ($scenario->{ccs_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$parent_cdrs->[1]->{source_user_id}))
                        and ($scenario->{ccs_subscriber}->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($scenario->{ccs_subscriber}->{id},1)->[0])
                        and (not defined $primary_alias_pattern or $scenario->{ccs_subscriber}->{primary_alias}->{username} =~ $primary_alias_pattern)
                        ) {
                        $scenario->{code} = $CFU;
                        $result = 1;
                    } #elsif (...
                    #
                    #}

                    foreach my $cdr (@scenario_cdrs) {
                        if ($result) {
                            $cdr->{_extended_export_status} = $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::OK;
                        } elsif ($malformed) {
                            $cdr->{_extended_export_status} = $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::FAILED;
                        } else {
                            $cdr->{_extended_export_status} = $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::SKIPPED;
                        }
                        $context->{file_cdr_id_map}->{$cdr->{id}} = $cdr; #->{start_time};
                        lock $rowcount;
                        $rowcount += 1;
                    }
                }
            }
        }
    }

    if ($scenario->{code} == $BLIND_TRANSFER_NO_IVR) {
        my $originating = $parent_cdrs->[0]->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $parent_cdrs->[0]->{$ama_terminating_digits_cdr_field};
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[1]->{start_time}, #?
            duration => $parent_cdrs->[1]->{duration},
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => ($parent_cdrs->[1]->{call_status} ne $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0),
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '0001',
            },
        });
    } elsif ($scenario->{code} == $BLIND_TRANSFER) {
        my $originating = $parent_cdrs->[0]->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $parent_cdrs->[0]->{$ama_terminating_digits_cdr_field};
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[0]->{start_time}, #?
            duration => abs($parent_cdrs->[0]->{start_time} - $parent_cdrs->[1]->{init_time}),
            originating => _rewrite_originating($originating),
            terminating => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : _rewrite_terminating($terminating)),
            terminating_cdr => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : $terminating),
            unanswered => 0,
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '6001',
            },
        },{
            start_time => $parent_cdrs->[1]->{start_time},
            duration => $parent_cdrs->[1]->{duration},
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => ($parent_cdrs->[1]->{call_status} ne $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0),
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '2002',
            },
        });
    } elsif ($scenario->{code} == $NO_TRANSFER_NO_IVR) {
        my $originating = $parent_cdrs->[0]->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[0]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $terminating;
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[0]->{start_time}, #?
            duration => 0,
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => 1,
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '0001',
            },
        });
    } elsif ($scenario->{code} == $NO_TRANSFER) {
        my $originating = $parent_cdrs->[0]->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[0]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $terminating;
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[0]->{start_time}, #?
            duration => $parent_cdrs->[0]->{duration},
            originating => _rewrite_originating($originating),
            terminating => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : _rewrite_terminating($terminating)),
            terminating_cdr => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : $terminating),
            unanswered => 0,
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '6001',
            },
        },{
            start_time => $parent_cdrs->[0]->{start_time}, #?
            duration => 0,
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => 1,
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '2002',
            },
        });
    } elsif ($scenario->{code} == $ATTN_TRANSFER_NO_IVR) {
        my $correlated_cdr = $parent_cdrs->[1]->{_correlated_cdrs}->[0];
        my $originating = $correlated_cdr->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $correlated_cdr->{$ama_terminating_digits_cdr_field};
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[1]->{start_time}, #?
            duration => $correlated_cdr->{duration} - abs($correlated_cdr->{start_time} - $parent_cdrs->[1]->{start_time}),
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => ($correlated_cdr->{call_status} ne $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0),
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '0001',
            },
        });
    } elsif ($scenario->{code} == $ATTN_TRANSFER) {
        my $correlated_cdr = $parent_cdrs->[1]->{_correlated_cdrs}->[0];
        my $originating = $correlated_cdr->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $correlated_cdr->{$ama_terminating_digits_cdr_field};
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[1]->{start_time}, #?
            duration => abs($correlated_cdr->{start_time} - $parent_cdrs->[1]->{init_time}),
            originating => _rewrite_originating($originating),
            terminating => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : _rewrite_terminating($terminating)),
            terminating_cdr => ($terminating_open_digits_6001 ? $terminating_open_digits_6001 : $terminating),
            unanswered => 0,
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '6001',
            },
        },{
            start_time => $parent_cdrs->[1]->{start_time}, #?
            duration => $correlated_cdr->{duration} - abs($correlated_cdr->{start_time} - $parent_cdrs->[1]->{start_time}),
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => ($correlated_cdr->{call_status} ne $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0),
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '2002',
            },
        });
    } elsif ($scenario->{code} == $CFU) {
        my $originating = $parent_cdrs->[0]->{$ama_originating_digits_cdr_field};
        my $terminating = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        my $switch_number = $parent_cdrs->[1]->{$ama_terminating_digits_cdr_field};
        push(@{$scenario->{ama}},{
            start_time => $parent_cdrs->[1]->{start_time}, #?
            duration => $parent_cdrs->[1]->{duration},
            originating => _rewrite_originating($originating),
            terminating => _rewrite_terminating($terminating),
            terminating_cdr => $terminating,
            unanswered => ($parent_cdrs->[1]->{call_status} ne $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0),
            correlation_id => substr($parent_cdrs->[0]->{id},-7),
            nod => {
                originating_digits => $originating,
                switch_number_digits => _rewrite_switch_number($switch_number), #$scenario->{ccs_subscriber}->{primary_alias}->{username},
                mode => '0001',
            },
        });
    }

    return $result;

}

sub _rewrite_switch_number {

    my ($switch_number) = @_;
    return _rewrite_number($switch_number,$switch_number_pattern,$switch_number_replacement);

}

sub _rewrite_originating {

    my ($originating) = @_;
    return _rewrite_number($originating,$originating_pattern,$originating_replacement);

}

sub _rewrite_terminating {

    my ($terminating) = @_;
    return _rewrite_number($terminating,$terminating_pattern,$terminating_replacement);

}

sub _rewrite_number {

    my ($number,$pattern,$replacement) = @_;
    if (defined $pattern and defined $replacement) {
        $number =~ s/$pattern/$replacement/;
    }
    return $number;

}

sub _commit_export_status {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};
    my $result = 1;
    _info($context,"file " . $context->{file}->get_filename(1) . " (" . kbytes2gigs(int($context->{file}->get_filesize() / 1024)) . ") - " . $context->{file}->get_record_count() . " records in " . $context->{file}->get_block_count() . " blocks");
    eval {
        ping_dbs();
        $context->{db}->db_begin();
        foreach my $id (keys %{$context->{file_cdr_id_map}}) {
            #mark exported
            NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::upsert_row($context->{db},
                cdr_id => $id,
                status_id => $context->{export_status_id},
                export_status => $context->{file_cdr_id_map}->{$id}->{_extended_export_status},
                cdr_start_time => $context->{file_cdr_id_map}->{$id}->{start_time},
            );
            _info($context,"export_status '$context->{file_cdr_id_map}->{$id}->{_extended_export_status}' set for cdr id $id",1);
        }
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::insert_system_mark($context->{db},
            $export_cdr_stream,
            $context->{file_sequence_number},
        ); #set mark...
        _info($context,"file sequence number $context->{file_sequence_number} saved");
        $context->{db}->db_commit();
    };
    $context->{file_cdr_id_map} = {};
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_rollback(1);
        };
        #eval {
        #    unlink $context->{file}->get_filename();
        #};
        die($err);
        $result = 0;
    } else {
        push(@{$context->{ama_files}},$context->{file}->get_filename());
        _increment_file_sequence_number($context) if $context->{has_next};
    }
    return $result;

}

sub _increment_file_sequence_number {
    my ($context) = @_;
    lock $file_sequence_number;
    $file_sequence_number = $file_sequence_number + 1;
    _info($context,"file sequence number incremented: $file_sequence_number",1);
    $context->{file_sequence_number} = $file_sequence_number;
}

sub _get_transfer_in {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};

    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013->new(

            rewritten => 0,
            sensor_id => $ama_sensor_id,

            padding => 0,
            recording_office_id => $ama_recording_office_id,

            #date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($context->{dt_transfer_in}),
            #connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($context->{dt_transfer_in}), # adjacent?

            file_sequence_number => $context->{file_sequence_number},
        )
    );

}

sub _create_ama_record {
    my ($context,$ama) = @_;
    $context->{file}->update_start_end_time($ama->{start_time},$ama->{start_time} + $ama->{duration});
    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510->new(
            call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::STATION_PAID,

            rewritten => 0,
            sensor_id => $ama_sensor_id,

            padding => 0,
            recording_office_id => $ama_recording_office_id,

            call_type => '970',
            #timing ind 000
            #seervice observed 0c

            unanswered => $ama->{unanswered}, #called party off-hook setzen

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date(from_epoch($ama->{start_time})),

            service_feature => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

            originating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($ama->{originating}),
            originating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($ama->{originating}),
            originating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($ama->{originating}),

            domestic_international => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational::get_number_domestic_international($ama->{terminating_cdr}),

            terminating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($ama->{terminating}),
            terminating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($ama->{terminating}),
            terminating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($ama->{terminating}),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time(from_epoch($ama->{start_time})),
            elapsed_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime::get_elapsed_time($ama->{duration}),
        ),
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module611->new(
            generic_context_identifier => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier::IN_CORRELATION_ID,
            parsing_rules => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier::IN_CORRELATION_ID_PARSING_RULES,
            additional_digits_dialed => $ama->{correlation_id},
        ),
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module199->new(
            network_operator_data => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::NetworkOperatorData::get_network_operator_data(
                $ama->{nod}->{originating_digits},
                $ama->{nod}->{switch_number_digits},
                $ama->{nod}->{mode},
                ),
        ),
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module104->new(
            direction => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification::INCOMING,
            trunk_group_number => $ama_incoming_trunk_group_number,
            trunk_member_number => '0000',
        ),
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module104->new(
            direction => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification::OUTGOING,
            trunk_group_number => $ama_outgoing_trunk_group_number,
            trunk_member_number => '0000',
        ),
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module000->new(
        ),
    );
}

sub _create_ama_records {
    my ($context,$scenario_name) = @_;
    my $records = [ map { _create_ama_record($context,$_); } @{$context->{scenario}->{ama}} ];
    _info($context,"$scenario_name - cdr ids " . join(', ', map { $_->{id}; } @{$context->{scenario}->{all_cdrs}}) . ':' . join("\n", map { $_->to_string(); } @$records),1);
    return $records;
}

sub _get_record {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};

    if ($context->{scenario}->{code} == $BLIND_TRANSFER_NO_IVR) {
        return _create_ama_records($context,'BLIND_TRANSFER_NO_IVR');
    } elsif ($context->{scenario}->{code} == $BLIND_TRANSFER) {
        return _create_ama_records($context,'BLIND_TRANSFER');
    } elsif ($context->{scenario}->{code} == $NO_TRANSFER_NO_IVR) {
        return _create_ama_records($context,'NO_TRANSFER_NO_IVR');
    } elsif ($context->{scenario}->{code} == $NO_TRANSFER) {
        return _create_ama_records($context,'NO_TRANSFER');
    } elsif ($context->{scenario}->{code} == $ATTN_TRANSFER_NO_IVR) {
        return _create_ama_records($context,'ATTN_TRANSFER_NO_IVR');
    } elsif ($context->{scenario}->{code} == $ATTN_TRANSFER) {
        return _create_ama_records($context,'ATTN_TRANSFER');
    } elsif ($context->{scenario}->{code} == $CFU) {
        return _create_ama_records($context,'CFU');
    } else {
        _error($context,"unknown scenario $context->{scenario}->{code} for cdr ids " . join(', ',map { $_->{id}; } @{$context->{scenario}->{all_cdrs}}) );
    }

}

sub _get_transfer_out {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};

    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014->new(
            @_,
            rewritten => 0,
            sensor_id => $ama_sensor_id,

            padding => 0,
            recording_office_id => $ama_recording_office_id,

            #date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($context->{dt_transfer_out}),
            #connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($context->{dt_transfer_out}),

            file_sequence_number => $context->{file_sequence_number},

        )
    );

}

sub _check_export_status_stream {

    my ($context) = @_;

    my $result = 1;

    my $export_status;
    eval {
        if ($export_cdr_stream) {
            $export_status = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status::findby_type($export_cdr_stream);
        }
        $context->{export_status_id} = $export_status->{id} if $export_status;
    };
    if ($@ or ($export_cdr_stream and not $export_status)) {
        _error($context,"cannot find export stream '$export_cdr_stream'");
        $result = 0;
    } elsif ($export_status) {
        _info($context,"using export stream '$export_cdr_stream'");
    }

    return $result;

}

sub _export_cdrs_create_context {

    my ($context) = @_;

    my $result = 1;

    $context->{tid} = threadid();

    $result &= _check_export_status_stream($context);

    $context->{file} = NGCP::BulkProcessor::Projects::Export::Ama::Format::File->new();
    $context->{file_cdr_id_map} = {};
    $context->{has_next} = 1;

    my $fsn;
    eval {
        $fsn = NGCP::BulkProcessor::Dao::Trunk::accounting::mark::get_system_mark(undef,
            $export_cdr_stream
        ); #load mark...
    };
    if ($@) {
        _error($context,"cannot get last file sequence number");
        $result = 0;
    } else {
        my $reset = 0;
        if (not defined $fsn) {
            $fsn = $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn - 1;
        } elsif ($fsn < 0) {
            $reset = 1;
        } elsif ($fsn >= $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn) {
            if ($export_cdr_rollover_fsn) {
                $reset = 1;
            } else {
                _warn($context,"file sequence number $fsn exceeding limit (" . $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn . ")");
                $result = 0;
            }
        } else {
            _info($context,"last file sequence number is $fsn");
        }
        if ($reset) {
            $fsn = $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn - 1;
            eval {
                NGCP::BulkProcessor::Dao::Trunk::accounting::mark::delete_system_marks(undef,
                    $export_cdr_stream,
                );
            };
            if ($@) {
                if ($skip_errors) {
                    _warn($context,"problem with file sequence number reset: " . $@);
                } else {
                    _error($context,"problem with file sequence number reset: " . $@);
                }
                $result = 0;
            } else {
                _info($context,"file sequence number deleted"); #reset to $fsn")
            }
        }
        lock $file_sequence_number;
        $file_sequence_number = $fsn;
    }

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
