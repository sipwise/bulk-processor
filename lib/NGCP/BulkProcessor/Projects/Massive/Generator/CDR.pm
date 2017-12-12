ackage NGCP::BulkProcessor::Projects::Massive::Generator::CDR;
use strict;

## no critic

use threads::shared qw();
use Time::HiRes qw(sleep);
use String::MkPasswd qw();
#use List::Util qw();

use Tie::IxHash;

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
    $cpucount
);

use NGCP::BulkProcessor::Projects::Massive::Generator::Settings qw(
    $dry
    $skip_errors
    $deadlock_retries

    $provision_subscriber_multithreading
    $provision_subscriber_numofthreads
    $provision_subscriber_count

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

use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    ping_dbs
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid timestamp); # stringtobool check_ipnet trim);
#use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
#use NGCP::BulkProcessor::RandomString qw(createtmpstring);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    provision_subscribers

);

my $thread_sleep_secs = 0.1;

sub set_barring_profiles {

    my $static_context = {};
    my $result = _set_barring_profiles_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $subscriber (@$records) {
                $rownum++;
                next unless _reset_set_barring_profile_context($context,$imported_subscriber,$rownum);
                _set_barring_profile($context);
            }

            #return 0;
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
            # below is not mandatory..
            _check_insert_tables();
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
        multithreading => $set_barring_profiles_multithreading,
        numofthreads => $set_barring_profiles_numofthreads,
    ),$warning_count);
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