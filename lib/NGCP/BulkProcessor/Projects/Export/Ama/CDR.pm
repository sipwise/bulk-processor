package NGCP::BulkProcessor::Projects::Export::Ama::CDR;
use strict;

## no critic

use threads::shared qw();
#use Time::HiRes qw(sleep);
#use String::MkPasswd qw();
#use List::Util qw();
#use Data::Rmap qw();

#use Tie::IxHash;

#use NGCP::BulkProcessor::Globals qw(
#    $enablemultithreading
#);

use NGCP::BulkProcessor::Projects::Export::Ama::Settings qw(

    $skip_errors

    $export_cdr_multithreading
    $export_cdr_numofthreads
    $export_cdr_blocksize
    $export_cdr_joins
    $export_cdr_conditions
    $export_cdr_limit
    $export_cdr_stream
);
#$dry
#$deadlock_retries
#@providers
#$generate_cdr_numofthreads
#$generate_cdr_count

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

#use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
#use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();

#use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::File qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Record qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
    ping_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid); # stringtobool check_ipnet trim);
##use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
##use NGCP::BulkProcessor::RandomString qw(createtmpstring);
#use NGCP::BulkProcessor::Array qw(array_to_map);

use NGCP::BulkProcessor::Calendar qw(current_local);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    export_cdrs
    reset_fsn
);

sub reset_fsn {

    my $result = 1;
    my $context = { tid => threadid(), warning_count => 0, error_count => 0, };
    eval {
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::cleanup_system_marks(undef,
            $export_cdr_stream,
        );
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::set_system_mark(undef,
            $export_cdr_stream,
            '0' #$NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn
        );
    };
    if ($@) {
        if ($skip_errors) {
            _warn($context,"problem with last file sequence number reset: " . $@);
        } else {
            _error($context,"problem with last file sequence number reset: " . $@);
        }
        $result = 0;
    }
    return $result;

}

sub export_cdrs {

    my $static_context = {};
    my $result = _export_cdrs_create_context($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    my $thread_num : shared = 0;
    my @ama_files : shared = ();
    return ($result && NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::process_unexported(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            $context->{rownum} = $row_offset;
            $context->{block_call_id_map} = { map { $_->[1] => 1; } @$records };
            foreach my $record (@$records) {
                return 0 if (defined $export_cdr_limit and $context->{rownum} >= $export_cdr_limit);
                return 0 if $context->{file_sequence_number} > $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn;
                my ($id,$call_id) = @$record;

                # skip if the cdr is pending for flushing to file:
                next if exists $context->{file_cdr_id_map}->{$id};
                # skip if call legs/data is incomplete:
                next unless _export_cdrs_init_context($context,$call_id);

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

            lock $thread_num;
            $context->{file_sequence_number} += $thread_num;
            $thread_num++;
            # below is not mandatory..
            #_check_insert_tables();
        },
        uninit_process_context_code => sub {
            my ($context)= @_;

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
                        _warn($context,"problem while closing " . $context->{file}->get_file_name() . ": " . $@);
                    } else {
                        _error($context,"problem while exporting " . $context->{file}->get_file_name() . ": " . $@);
                    }
                }
            }

            eval {
                NGCP::BulkProcessor::Dao::Trunk::accounting::mark::cleanup_system_marks($context->{db},
                $export_cdr_stream);
            };
            if ($@) {
                if ($skip_errors) {
                    _warn($context,"problem with last file sequence number cleanup: " . $@);
                } else {
                    _error($context,"problem with last file sequence number cleanup: " . $@);
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
        limit => $export_cdr_limit,
    ),$warning_count,\@ama_files);
}


sub _export_cdrs_init_context {

    my ($context,$call_id) = @_;

    my $result = 1;

    $context->{call_id} = $call_id;
    if (exists $context->{block_call_id_map}->{$call_id}) {
        $context->{cdrs} = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callidprefix($context->{db},
            $call_id,$export_cdr_joins,$export_cdr_conditions);
        delete $context->{block_call_id_map}->{$call_id};
        #$result &= ((scalar @{$context->{cdrs}}) == 4 ? 1 : 0);
        $result &= ((scalar @{$context->{cdrs}}) > 0 ? 1 : 0);
        foreach my $cdr (@{$context->{cdrs}}) {
            if (exists $context->{file_cdr_id_map}->{$cdr->{id}}) {
                # skip if the cdr is pending for flushing to file:
                $result = 0;
                last;
            } else {
                $context->{file_cdr_id_map}->{$cdr->{id}} = $cdr->{start_time};
            }
            $context->{rownum} += 1;
        }
    } else {
        # skip if the cdr belongs to a call already done in this block: (for performance reasons)
        $context->{cdrs} = undef;
        $result = 0;
    }

    $context->{dt} = current_local();
    $context->{source} = "43011001";
    $context->{destination} = "43011002";
    $context->{duration} = 123.456;

    return $result;

}

sub _commit_export_status {

    my %params = @_;
    my (
        $context,
    ) = @params{qw/
        context
    /};
    #my %dropped = ();
    eval {
        ping_dbs();
        $context->{db}->db_begin();
        foreach my $id (keys %{$context->{file_cdr_id_map}}) {
            #mark exported
            NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::upsert_row($context->{db},
                cdr_id => $id,
                status_id => $context->{export_status_id},
                export_status => $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::EXPORTED,
                cdr_start_time => $context->{file_cdr_id_map}->{$id}, #->{start_time},
            );
            _info($context,"export_status set for cdr id $id",1);
            #$dropped{$cdr_id} = delete $context->{file_cdrs}->{$cdr_id};
        }
        NGCP::BulkProcessor::Dao::Trunk::accounting::mark::insert_system_mark($context->{db},
            $export_cdr_stream,
            $context->{file_sequence_number}
        ); #set mark...
        if ($export_cdr_multithreading) {
            $context->{file_sequence_number} += $export_cdr_numofthreads;
        } else {
            $context->{file_sequence_number} += 1;
        }
        $context->{db}->db_commit();

    };
    $context->{file_cdr_id_map} = {};
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_rollback(1);
            #foreach (keys %dropped) {
            #    $cdr_id_map{$_} = $dropped{$_};
            #}
        };
        eval {
            unlink $context->{file}->get_filename();
        };
        die($err);
    } else {
        push(@{$context->{ama_files}},$context->{file}->get_filename());
    }

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
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($context->{dt}),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($context->{dt}),

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
    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510->new(
            call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::STATION_PAID,

            rewritten => 0,
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($context->{dt}),

            service_feature => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

            originating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($context->{source}),
            originating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($context->{source}),
            originating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($context->{source}),

            domestic_international => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational::get_number_domestic_international($context->{destination}),

            terminating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($context->{destination}),
            terminating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($context->{destination}),
            terminating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($context->{destination}),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($context->{dt}),
            elapsed_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime::get_elapsed_time($context->{duration}),
        )
    );

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

            rewritten => 0,
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($context->{dt}),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($context->{dt}),

            file_sequence_number => $context->{file_sequence_number},

            #=> (scalar @records),
        )
    );

}

sub _export_cdrs_create_context {

    my ($context) = @_;

    my $result = 1;

    $context->{tid} = threadid();

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

    $context->{file} = NGCP::BulkProcessor::Projects::Export::Ama::Format::File->new();
    $context->{file_cdr_id_map} = {};

    eval {
        $context->{file_sequence_number} = NGCP::BulkProcessor::Dao::Trunk::accounting::mark::get_system_mark(undef,
            $export_cdr_stream
        ); #load mark...
    };
    if ($@) {
        _error($context,"cannot get last file sequence number");
        $result = 0;
    } else {
        if ($context->{file_sequence_number} < $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn) {
            $context->{file_sequence_number} = $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::min_fsn;
        } elsif ($context->{file_sequence_number} >= $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn) {
            _error($context,"file sequence number $context->{file_sequence_number} is greater than " . $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::FileSequenceNumber::max_fsn);
            $result = 0;
        } else {
            $context->{file_sequence_number} += 1;
        }
        _info($context,"next file sequence number is $context->{file_sequence_number}");
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