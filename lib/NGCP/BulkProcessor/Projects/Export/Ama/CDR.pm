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

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
);
#ping_dbs

#use NGCP::BulkProcessor::Utils qw(threadid timestamp); # stringtobool check_ipnet trim);
##use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
##use NGCP::BulkProcessor::RandomString qw(createtmpstring);
#use NGCP::BulkProcessor::Array qw(array_to_map);

use NGCP::BulkProcessor::Calendar qw(current_local);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    export_cdrs

);

sub export_cdrs {

    my $static_context = {};
    my $result = _export_cdrs_create_context($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::process_unexported(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            $context->{rownum} = $row_offset;
            $context->{block_cdr_id_map} = { map { $_->[0] => $_->[1]; } @$records };
            foreach my $record (@$records) {
                return 0 if (defined $export_cdr_limit and $context->{rownum} >= $export_cdr_limit);
                my ($id,$call_id) = @$record;
                # skip if the cdr belongs to a call already done in this block:
                next unless exists $context->{block_cdr_id_map}->{$id};
                # skip if the cdr is pending for flushing to file:
                next if exists $context->{file_cdr_id_map}->{$id};
                # skip if call legs/data is incomplete:
                next unless _export_cdrs_init_context($context,$call_id);
                # go ahead:
                foreach my $cdr (@{$context->{cdrs}}) {
                    $context->{file_cdr_id_map}->{$cdr->{id}} = $cdr->{start_time};
                    delete $context->{cdr_id_map}->{$cdr->{id}};
                    $context->{rownum} += 1;
                }
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

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            # below is not mandatory..
            #_check_insert_tables();
        },
        uninit_process_context_code => sub {
            my ($context)= @_;

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

            undef $context->{db};
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 0,
        blocksize => $export_cdr_blocksize,
        multithreading => $export_cdr_multithreading,
        numofthreads => 1,
        joins => $export_cdr_joins,
        conditions => $export_cdr_conditions,
        #sort => [{ column => 'id', numeric => 1, dir => 1 }],
        limit => $export_cdr_limit,
    ),$warning_count);
}


sub _export_cdrs_init_context {

    my ($context,$call_id) = @_;

    my $result = 1;

    $context->{call_id} = $call_id;
    $context->{cdrs} = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callidprefix($context->{db},
            $call_id,$export_cdr_joins,$export_cdr_conditions);
    $result &= ((scalar @{$context->{cdrs}}) > 0 ? 1 : 0);

    #$result &= ((scalar @{$context->{cdrs}}) == 4 ? 1 : 0);

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
        $context->{db}->db_begin();
        foreach my $id (keys %{$context->{block_cdr_id_map}}) {
            #mark exported
            NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::upsert_row($context->{db},
                cdr_id => $id,
                status_id => $context->{export_status_id},
                export_status => $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::EXPORTED,
                cdr_start_time => $context->{block_cdr_id_map}->{$id}->{start_time},
            );
            _info($context,"export_status set for cdr id $id",1);
            #$dropped{$cdr_id} = delete $context->{file_cdrs}->{$cdr_id};
        }

        $context->{db}->db_commit();

    };
    $context->{block_cdr_id_map} = {};
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

            file_sequence_number => 1,
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

            #file_sequence_number => 1,

            #=> (scalar @records),
        )
    );

}

sub _export_cdrs_create_context {

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
        _info($context,"export stream '$export_cdr_stream' set");
    }

    $context->{file} = NGCP::BulkProcessor::Projects::Export::Ama::Format::File->new();
    $context->{file_cdr_id_map} = {};

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