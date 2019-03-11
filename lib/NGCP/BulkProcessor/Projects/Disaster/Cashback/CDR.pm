package NGCP::BulkProcessor::Projects::Disaster::Cashback::CDR;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Disaster::Cashback::Settings qw(

    $skip_errors

    $cashback_multithreading
    $cashback_numofthreads
    $cashback_blocksize

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


use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    destroy_dbs
    ping_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);

#use NGCP::BulkProcessor::Calendar qw(from_epoch);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    cashback
);

sub cashback {

    my ($from,$to) = @_;

    my $static_context = {};
    my $result = _cashback_create_context($static_context);

    destroy_dbs();
    my $warning_count :shared = 0;
    $result &= NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::process_fromto(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
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

        },
        uninit_process_context_code => sub {
            my ($context)= @_;

            undef $context->{db};
            destroy_dbs();

            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 0,
        blocksize => $cashback_blocksize,
        multithreading => cashback_multithreading,
        numofthreads => $cashback_numofthreads,
        from => $from,
        to => $to
    );

    return ($result,$warning_count);
}


sub _cashback_init_context {

    my ($context,$cdr_id,$call_id) = @_;

    my $result = 0;

    

    return $result;

}

sub _cashback_create_context {

    my ($context) = @_;

    my $result = 1;

    $context->{tid} = threadid();



    return $result;
}

sub _error {

    my ($context,$message) = @_;
    if ($skip_errors) {
        $context->{warning_count} = $context->{warning_count} + 1;
        rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        $context->{error_count} = $context->{error_count} + 1;
        rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));
    }
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