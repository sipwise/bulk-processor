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

my $DIRECT_FORWARDER_SCENARIO = 1;

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
            $context->{block_call_id_map} = {};
            foreach my $record (@$records) {
                my ($id,$call_id) = @$record;
                my $call_id_prefix = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::get_callidprefix($call_id);
                if (exists $context->{block_call_id_map}->{$call_id_prefix}) {
                    $context->{block_call_id_map}->{$call_id_prefix} += 1;
                } else {
                    $context->{block_call_id_map}->{$call_id_prefix} = 1;
                }
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


sub _export_cdrs_init_context {

    my ($context,$cdr_id,$call_id) = @_;

    my $result = 0;
    $context->{cdrs} = [];
    $context->{call_id} = $call_id;
    my $scenario = { code => 0, };
    $context->{scenario} = $scenario;

    if (not exists $context->{file_cdr_id_map}->{$cdr_id}) {
        my $call_id_prefix = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::get_callidprefix($call_id);
        if (exists $context->{block_call_id_map}->{$call_id_prefix}) {
            $context->{cdrs} = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callidprefix($context->{db},
                $call_id,$export_cdr_joins,$export_cdr_conditions); #already sorted
            my $cdrs_in_block = delete $context->{block_call_id_map}->{$call_id_prefix};
            if ((scalar @{$context->{cdrs}}) == $cdrs_in_block) {
                if ((scalar @{$context->{cdrs}}) == 2
                    and not $context->{cdrs}->[0]->is_xfer()
                    and $context->{cdrs}->[1]->is_xfer()
                    and ($scenario->{ccs_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$context->{cdrs}->[0]->{destination_user_id}))
                    and ($scenario->{ccs_subscriber}->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($scenario->{ccs_subscriber}->{id},1)->[0])
                    ) {
                    $scenario->{code} = $DIRECT_FORWARDER_SCENARIO;
                    $result = 1;
                #} else {
                #    print "blah";
                }
                foreach my $cdr (@{$context->{cdrs}}) {
                    if ($result) {
                        $cdr->{_extended_export_status} = $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::EXPORTED;
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

    if ($scenario->{code} == $DIRECT_FORWARDER_SCENARIO) {
        $scenario->{start_time} = $context->{cdrs}->[0]->{start_time};
        $scenario->{duration} = $context->{cdrs}->[0]->{duration};
        $scenario->{originating} = $context->{cdrs}->[0]->{$ama_originating_digits_cdr_field};
        $scenario->{terminating} = $context->{cdrs}->[1]->{$ama_terminating_digits_cdr_field};
        $scenario->{unanswered} = ($context->{cdrs}->[1]->{call_status} != $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::OK_CALL_STATUS ? 1 : 0);
        $scenario->{correlation_id} = substr($context->{cdrs}->[0]->{id},-7);
        $scenario->{nod} = {
            originating_digits => $scenario->{originating},
            switch_number_digits => $scenario->{ccs_subscriber}->{primary_alias}->{username},
            mode => '0001',
        };
    }

    return $result;

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

sub _get_record {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};

    if ($context->{scenario}->{code} == $DIRECT_FORWARDER_SCENARIO) {
        $context->{file}->update_start_end_time($context->{scenario}->{start_time},$context->{scenario}->{start_time} + $context->{scenario}->{duration});
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

                unanswered => $context->{scenario}->{unanswered}, #called party off-hook setzen

                date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date(from_epoch($context->{scenario}->{start_time})),

                service_feature => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

                originating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($context->{scenario}->{originating}),
                originating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($context->{scenario}->{originating}),
                originating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($context->{scenario}->{originating}),

                domestic_international => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational::INTERNATIONAL, #get_number_domestic_international($context->{destination}),

                terminating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($context->{scenario}->{terminating}),
                terminating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($context->{scenario}->{terminating}),
                terminating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($context->{scenario}->{terminating}),

                connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time(from_epoch($context->{scenario}->{start_time})),
                elapsed_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime::get_elapsed_time($context->{scenario}->{duration}),
            ),
            NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module611->new(
                generic_context_identifier => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier::IN_CORRELATION_ID,
                parsing_rules => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::GenericContextIdentifier::IN_CORRELATION_ID_PARSING_RULES,
                additional_digits_dialed => $context->{scenario}->{correlation_id},
            ),
            NGCP::BulkProcessor::Projects::Export::Ama::Format::Modules::Module199->new(
                network_operator_data => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::NetworkOperatorData::get_network_operator_data(
                    $context->{scenario}->{nod}->{originating_digits},
                    $context->{scenario}->{nod}->{switch_number_digits},
                    $context->{scenario}->{nod}->{mode},
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
    } else {
        _error($context,"unknown scenario $context->{scenario}->{code} for cdr ids " . join(', ',map { $_->{id}; } @{$context->{cdrs}}) );
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