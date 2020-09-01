package NGCP::BulkProcessor::Projects::ETL::Lnp::Import;
use strict;

## no critic

use threads::shared qw();

#use Encode qw();

use NGCP::BulkProcessor::Projects::ETL::Lnp::Settings qw(
    $import_multithreading

    $lnp_filename
    $lnp_rownum_start
    $lnp_import_numofthreads
    $ignore_lnp_unique
    $lnp_import_single_row_txn
    
    $expand_numbers_code

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
use NGCP::BulkProcessor::Projects::ETL::Lnp::FileProcessors::NumbersFile qw();

use NGCP::BulkProcessor::Projects::ETL::Lnp::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp qw();

use NGCP::BulkProcessor::Utils qw(threadid trim);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    load_file
);

sub load_file {

    my $result = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::create_table(0);

    my $importer;
    if (defined $expand_numbers_code) {
        $importer = NGCP::BulkProcessor::Projects::ETL::Lnp::FileProcessors::NumbersFile->new($lnp_import_numofthreads);
    } else {
        $importer = NGCP::BulkProcessor::FileProcessors::CSVFileSimple->new($lnp_import_numofthreads);
    }

    my $upsert = _lnp_reset_delta();
    
    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    return ($result && $importer->process(
        file => $lnp_filename,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            $context->{lnp_rows} = [];
            foreach my $row (@$rows) {
                $rownum++;
                next if (defined $lnp_rownum_start and $rownum < $lnp_rownum_start);
                next if (scalar @$row) == 0;
                #$row = [ map { local $_ = $_; trim($_); $_ =~ s/^"//; $_ =~ s/"$//r; } @$row ];
                my $record = NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp->new([
                    map { local $_ = $_; trim($_); $_ =~ s/^"//; $_ =~ s/"$//r; } @$row
                ]);
                #$record->{number} = $record->{cc} . $record->{ac} . $record->{sn};
            
                my %r = %$record; my @row_ext = @r{@NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::fieldnames};
                if ($context->{upsert}) {
                    push(@row_ext,$record->{number});
                } else {
                    push(@row_ext,$NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::added_delta);
                }
                push(@{$context->{lnp_rows}},\@row_ext);
                if ($lnp_import_single_row_txn and (scalar @{$context->{lnp_rows}}) > 0) {
                    while (defined (my $lnp_row = shift @{$context->{lnp_rows}})) {
                        if ($skip_errors) {
                            eval { _insert_lnp_rows($context,[$lnp_row]); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_lnp_rows($context,[$lnp_row]);
                        }
                    }
                }                
            }

            if (not $lnp_import_single_row_txn and (scalar @{$context->{lnp_rows}}) > 0) {
                if ($skip_errors) {
                    eval { _insert_lnp_rows($context,$context->{lnp_rows}); };
                    _warn($context,$@) if $@;
                } else {
                    _insert_lnp_rows($context,$context->{lnp_rows});
                }
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_sqlite_db();
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
        multithreading => $import_multithreading,
    ),$warning_count);

}

sub _insert_lnp_rows {
    my ($context,$lnp_rows) = @_;
    $context->{db}->db_do_begin(($context->{upsert} ?
        NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::getupsertstatement()
        : NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::getinsertstatement($ignore_lnp_unique)),
    );
    eval {
        $context->{db}->db_do_rowblock($lnp_rows);
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

sub _lnp_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::has_rows()) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::update_delta(undef,
            $NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp::deleted_delta) .
            ' lnp records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
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
