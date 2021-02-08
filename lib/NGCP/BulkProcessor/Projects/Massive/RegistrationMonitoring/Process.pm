package NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Process;
use strict;

## no critic

use threads::shared qw();

#use Encode qw();

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Settings qw(

    $usernames_filename
    $usernames_rownum_start
    $load_registrations_multithreading
    $load_registrations_numofthreads
    $ignore_location_unique
    $location_single_row_txn

    $skip_errors

);
use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingwarn
    fileprocessingerror
);

use NGCP::BulkProcessor::FileProcessors::CSVFileSimple qw();
#use NGCP::BulkProcessor::FileProcessors::XslxFileSimple qw();

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);
use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_stores
);

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location qw();

use NGCP::BulkProcessor::Redis::Trunk::location::usrdom qw();

use NGCP::BulkProcessor::Utils qw(threadid trim);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    load_registrations
);

sub load_registrations {
    
    my ($file) = @_;
    $file //= $usernames_filename;
    _error({ filename => '<none>', },'no file specified') unless $file;

    my $result = NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::create_table(0);

    my $importer = NGCP::BulkProcessor::FileProcessors::CSVFileSimple->new($load_registrations_numofthreads,undef,';');

    my $upsert = _locations_reset_delta();
    
    destroy_all_dbs(); #close all db connections before forking..
    destroy_stores();
    my $warning_count :shared = 0;
    return ($result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my @data = ();
            my @location_rows = ();
            foreach my $record (@$records) {
                if (_load_registrations_init_context($context,$record->[0],$record->[1])) {
                    foreach my $registration (@{$context->{registrations}}) {
                        my %r = %{$registration->getvalue}; my @row_ext = @r{@NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::fieldnames};
                        if ($context->{upsert}) {
                            push(@row_ext,$registration->getvalue()->{ruid});
                        } else {
                            push(@row_ext,$NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::added_delta);
                        }
                        push(@location_rows,\@row_ext);
                    }
                    if ($location_single_row_txn and (scalar @location_rows) > 0) {
                        while (defined (my $location_row = shift @location_rows)) {
                            if ($skip_errors) {
                                eval { _insert_location_rows($context,[$location_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_location_rows($context,[$location_row]);
                            }
                        }
                    }
                }
            }
            if (not $location_single_row_txn and (scalar @location_rows) > 0) {
                if ($skip_errors) {
                    eval { _insert_location_rows($context,\@location_rows); };
                    _warn($context,$@) if $@;
                } else {
                    _insert_location_rows($context,\@location_rows);
                }
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_sqlite_db();
            #$context->{redis} = &get_location_store();
            $context->{upsert} = $upsert;
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            #undef $context->{redis};
            destroy_all_dbs();
            destroy_stores();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        multithreading => $load_registrations_multithreading,
        numofthreads => $load_registrations_numofthreads,
    ),$warning_count);

}

sub _locations_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::countby_delta() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::deleted_delta) .
            ' location records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_location_rows {
    my ($context,$location_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location::getinsertstatement($ignore_location_unique)),
    );
    eval {
        $context->{db}->db_do_rowblock($location_rows);
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

sub _load_registrations_init_context() {
    
    my ($context,$username,$domain) = @_;
    $context->{username} = $username;
    $context->{domain} = $domain;
    my @registrations = ();
    my $result = 1;
    $context->{usrdom} = NGCP::BulkProcessor::Redis::Trunk::location::usrdom::get_usrdom_by_username_domain($username,$domain,{ _entries => 1, });
    if ($context->{usrdom}) {
        foreach my $entry (@{$context->{usrdom}->{_entries}}) {
            push(@registrations,$entry); # if expiry > now
        }
    }
    $result = 0 unless scalar @registrations;
    $context->{registrations} = \@registrations;
    return $result;
    
}




sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    fileprocessingerror($context->{filename},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    fileprocessingwarn($context->{filename},$message,getlogger(__PACKAGE__));

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
