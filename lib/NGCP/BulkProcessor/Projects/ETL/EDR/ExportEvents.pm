package NGCP::BulkProcessor::Projects::ETL::EDR::ExportEvents;
use strict;

## no critic

use threads::shared qw();

use Tie::IxHash;

#use NGCP::BulkProcessor::Serialization qw();
#use Scalar::Util qw(blessed);
#use MIME::Base64 qw(encode_base64);

use NGCP::BulkProcessor::Projects::ETL::EDR::Settings qw(
    $dry
    $skip_errors

    $export_subscriber_profiles_multithreading
    $export_subscriber_profiles_numofthreads
    $export_subscriber_profiles_blocksize
    $export_subscriber_profiles_joins
    $export_subscriber_profiles_conditions
    $export_subscriber_profiles_limit

    $period_events_single_row_txn
    $ignore_period_events_unique

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

use NGCP::BulkProcessor::Dao::Trunk::accounting::events qw();

use NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents qw();

use NGCP::BulkProcessor::Projects::ETL::EDR::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
    ping_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp stringtobool trim); #check_ipnet
use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
use NGCP::BulkProcessor::Array qw(contains);
use NGCP::BulkProcessor::Calendar qw(from_epoch datetime_to_string);
#use NGCP::BulkProcessor::DSPath qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    export_subscriber_profiles
);

sub export_subscriber_profiles {

    my $result = NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents::create_table(1);
    
    my $static_context = {};
    
    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::Dao::Trunk::accounting::events::process_subscribers(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            my @period_event_rows = ();
            foreach my $subscriber_id (map { $_->[0]; } @$records) {
                if ($subscriber_id == 202) {
                    my $x=1;
                    print "blah";
                }
                next unless _export_subscriber_profiles_init_context($context,$subscriber_id);
                push(@period_event_rows, _get_period_event_rows($context));
                
                
                
                if ($period_events_single_row_txn and (scalar @period_event_rows) > 0) {
                    while (defined (my $period_event_row = shift @period_event_rows)) {
                        if ($skip_errors) {
                            eval { _insert_period_events_rows($context,[$period_event_row]); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_period_events_rows($context,[$period_event_row]);
                        }
                    }
                }
            }
            
            if (not $period_events_single_row_txn and (scalar @period_event_rows) > 0) {
                if ($skip_errors) {
                    eval { insert_period_events_rows($context,\@period_event_rows); };
                    _warn($context,$@) if $@;
                } else {
                    insert_period_events_rows($context,\@period_event_rows);
                }
            }
            
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_sqlite_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
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
        destroy_reader_dbs_code => \&destroy_all_dbs,
        blocksize => $export_subscriber_profiles_blocksize,
        multithreading => $export_subscriber_profiles_multithreading,
        numofthreads => $export_subscriber_profiles_numofthreads,
        joins => $export_subscriber_profiles_joins,
        conditions => $export_subscriber_profiles_conditions,
        #sort => [{ column => 'id', numeric => 1, dir => 1 }],
        #limit => $export_subscriber_profiles_limit,
    ),$warning_count,);

}

sub _export_subscriber_profiles_init_context {

    my ($context,$subscriber_id) = @_;

    my $result = 1;
    
    $context->{events} = NGCP::BulkProcessor::Dao::Trunk::accounting::events::findby_subscriberid(
        undef,$subscriber_id,$export_subscriber_profiles_joins,$export_subscriber_profiles_conditions);
    
    $context->{subscriber_id} = $subscriber_id;
    
    return $result;

}

sub _get_period_event_rows {
    
    my ($context) = @_;
    
    my $profile_events = {
        start => undef,
        update => [],
        stop => undef,
    };
    my $last_event;

    my %subscriber_profiles = ();
    tie(%subscriber_profiles, 'Tie::IxHash');
    
    foreach my $event (@{sort_by_configs([ grep { contains($_->{type},[ qw(start_profile update_profile end_profile) ]); } @{$context->{events}} ],[
            {   numeric     => 1,
                dir         => 1, #-1,
                memberchain => [ 'id' ],
            }
        ])}) {
        if ($event->{type} eq 'start_profile') {
            if (not defined $last_event or $last_event->{type} eq 'end_profile') {
                $profile_events->{start} = $event;
                $last_event = $event;
                $subscriber_profiles{$event->{new_status}} = $profile_events;
            } else {
                
            }
        } elsif ($event->{type} eq 'update_profile') {
            if (defined $last_event and contains($last_event->{type},[ qw(start_profile update_profile) ])) {
                push(@{$profile_events->{update}},$event);
                $last_event = $event;
            } else {
                
            }
        } elsif ($event->{type} eq 'end_profile') {
            if (defined $last_event and contains($last_event->{type},[ qw(start_profile update_profile) ])) {
                $profile_events->{stop} = $event;
                $last_event = $event;
                $profile_events = {
                    start => undef,
                    update => [],
                    stop => undef,
                };
            } else {
                
            }
        }
    }
    
    my @period_event_rows = ();
    foreach my $profile_id (keys %subscriber_profiles) {
        $profile_events = $subscriber_profiles{$profile_id};
        push(@period_event_rows,[
            $context->{subscriber_id},
            $profile_id,
            datetime_to_string(from_epoch($profile_events->{start}->{timestamp})),
            join(",",map { datetime_to_string(from_epoch($_)); } @{$profile_events->{update}}),
            (defined $profile_events->{stop} ? datetime_to_string(from_epoch($profile_events->{stop}->{timestamp})) : undef),
        ]);
    }
    
    return @period_event_rows;
    
}

sub _insert_period_events_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents::getinsertstatement($ignore_period_events_unique),
    );
    eval {
        $context->{db}->db_do_rowblock($subscriber_rows);
        $context->{db}->db_finish();
    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_finish(1);
        };
        die($err);
    }

}


sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    }
}

sub _debug {

    my ($context,$message,$debug) = @_;
    processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

1;
