package NGCP::BulkProcessor::Projects::Migration::Teletek::Import;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Settings qw(
    $import_multithreading
    $flatfile_import_numofthreads
    $ignore_flatfile_unique
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

use NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_flatfile
);

sub import_flatfile {

    my ($file) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::create_table(0);

    $result &= _import_flatfile_checks($file);

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($flatfile_import_numofthreads);

    my $upsert = _import_flatfile_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    return ($result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @flatfile_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                next if (scalar @$row) == 0;
                my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile->new($row);
                my @flatfile_row = @$row;
                if ($context->{upsert}) {
                    push(@flatfile_row,$record->{cc},$record->{ac},$record->{sn});
                } else {
                    push(@flatfile_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::added_delta);
                }
                push(@flatfile_rows,\@flatfile_row);
            }

            #if ((scalar @flatfile_rows) > 0) {
            #    if ($skip_errors) {
            #        eval { _insert_flatfile_rows($context,\@flatfile_rows); };
            #    } else {
            #        _insert_flatfile_rows($context,\@flatfile_rows);
            #    }
            #}

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
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
        multithreading => $import_multithreading
    ),$warning_count);

}

sub _import_flatfile_checks {
    my ($file) = @_;
    my $result = 1;
    #my $optioncount = 0;
    #eval {
    #    $optioncount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::FeatureOption::countby_subscribernumber_option();
    #};
    #if ($@ or $optioncount == 0) {
    #    fileprocessingerror($file,'please import subscriber features first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}
    #my $userpasswordcount = 0;
    #eval {
    #    $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::UsernamePassword::countby_fqdn();
    #};
    #if ($@ or $userpasswordcount == 0) {
    #    fileprocessingerror($file,'please import user passwords first',getlogger(__PACKAGE__));
    #    $result = 0; #even in skip-error mode..
    #}
    return $result;
}

sub _import_flatfile_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::countby_ccacsn() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::deleted_delta) .
            ' flatfile records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_flatfile_rows {
    my ($context,$flatfile_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Flatfile::getinsertstatement($ignore_flatfile_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
        #lock
    );
    eval {
        $context->{db}->db_do_rowblock($flatfile_rows);
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
