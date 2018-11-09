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

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
);
#ping_dbs

#use NGCP::BulkProcessor::Utils qw(threadid timestamp); # stringtobool check_ipnet trim);
##use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
##use NGCP::BulkProcessor::RandomString qw(createtmpstring);
#use NGCP::BulkProcessor::Array qw(array_to_map);

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
            $context->{cdr_id_map} = { map { $_->[0] => $_->[1]; } @$records };
            foreach my $record (@$records) {
                return 0 if (defined $export_cdr_limit and $rownum >= $export_cdr_limit);
                my ($id,$call_id) = @$record;
                next unless exists $context->{cdr_id_map}->{$id};
                next if exists $context->{file_cdrs}->{$id};
                next unless _export_cdrs_init_context($context,$call_id);


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

    $context->{cdrs} = NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::findby_callidprefix($context->{db},
            $call_id,$export_cdr_joins,$export_cdr_conditions);
    $result = 0 unless (scalar @{$context->{cdrs}}) == 4;


    return $result;

}

sub x {
    my $context = shift;

        #$context->{file_cdrs} //= [];
    foreach my $cdr (@{$context->{cdrs}}) {
        #$context->{file_cdrs}->{$cdr->{id}} = $cdr->{start_time};
        push(@{$context->{file_cdrs}},$cdr);
        delete $context->{cdr_id_map}->{$cdr->{id}};
    }

    $context->{file}->write_record(
        get_transfer_in => \&_get_transfer_in,
        get_record => \&_get_record,
        get_transfer_out => \&_get_transfer_out,
        commit_cb => \&_commit_export_status,
        context => $context,
    );





}

sub _commit_export_status {

    my %params = @_;
    (
        $context,
    ) = @params{qw/
        context
    /};
    #my %dropped = ();
    eval {
        $context->{db}->db_begin();
        foreach my $cdr (@{$context->{file_cdrs}}) {
            #mark exported
            NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::upsert_row($context->{db},
                cdr_id => $cdr->{id},
                status_id => $context->{export_status_id},
                export_status => $NGCP::BulkProcessor::Dao::Trunk::accounting::cdr_export_status_data::EXPORTED,
                cdr_start_time => $cdr->{start_time},
            );
            _info($context,"export_status set for cdr id $cdr->{id}",1);
            #$dropped{$cdr_id} = delete $context->{file_cdrs}->{$cdr_id};
        }

        $context->{db}->db_commit();
        $context->{file_cdrs} = [];
    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_rollback(1);
            #foreach (keys %dropped) {
            #    $cdr_id_map{$_} = $dropped{$_};
            #}
        };
        die($err);
        if ($skip_errors) {
            _warn($context,"problem while exporting call id $call_id (cdr id $id): " . $err);
        } else {
            _error($context,"problem while exporting call id $call_id (cdr id $id): " . $err);
        }
    }

}

sub _get_transfer_in {

    my %params = @_;
    (
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

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),

            file_sequence_number => 1,
        )
    );

}

sub _get_record {

    my %params = @_;
    (
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

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            service_feature => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

            originating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($source),
            originating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($source),
            originating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($source),

            domestic_international => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational::get_number_domestic_international($destination),

            terminating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($destination),
            terminating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($destination),
            terminating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($destination),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),
            elapsed_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime::get_elapsed_time($duration),
        )
    );

}

sub _get_transfer_out {

    my %params = @_;
    (
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

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),

            file_sequence_number => 1,

            => (scalar @records),
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
    $context->{file_cdrs} = [];

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