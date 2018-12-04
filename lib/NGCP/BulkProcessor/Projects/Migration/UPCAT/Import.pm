package NGCP::BulkProcessor::Projects::Migration::UPCAT::Import;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::UPCAT::Settings qw(
    $provision_subscriber_rownum_start
    $import_multithreading
    $subscriber_import_numofthreads
    $ignore_subscriber_unique
    $subscriber_import_single_row_txn

    $skip_errors

    $default_reseller_name
    $default_billing_profile_name
    $default_barring
    $cc_ac_map
    $default_cc
    $cc_len_min
    $cc_len_max
    $ac_len
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

#use NGCP::BulkProcessor::Projects::Migration::UPCAT::FileProcessors::CSVFile qw();
use NGCP::BulkProcessor::FileProcessors::CSVFileSimple qw();

use NGCP::BulkProcessor::Projects::Migration::UPCAT::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(threadid zerofill trim);
use NGCP::BulkProcessor::Table qw(get_rowhash);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_subscriber

);

sub import_subscriber {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::create_table(0);

    foreach my $file (@files) {
        $result &= _import_subscriber_checks($file);
    }

    #my $importer = NGCP::BulkProcessor::Projects::Migration::UPCAT::FileProcessors::CSVFile->new($subscriber_import_numofthreads);
    my $importer = NGCP::BulkProcessor::FileProcessors::CSVFileSimple->new($subscriber_import_numofthreads);

    my $upsert = _import_subscriber_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    my $filenum = 0;
    foreach my $file (@files) {
        $filenum++;
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @subscriber_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (defined $provision_subscriber_rownum_start and $rownum < $provision_subscriber_rownum_start);
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); $_ =~ s/^"//; $_ =~ s/"$//r; } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber->new($row);
                    $record->{reseller_name} = $default_reseller_name;
                    ($record->{sip_username},$record->{domain}) = split('@',$record->{_txt_sw_username},2);
                    $record->{billing_profile_name} = $default_billing_profile_name;
                    ($record->{cc},$record->{ac},$record->{sn}) = _split_dn($record->{_dn});
                    $record->{web_username} = undef;
                    $record->{web_password} = undef;
                    $record->{barring} = $default_barring;
                    #$record->{allowed_ips}
                    #"channels",
                    #"voicemail",

                    $record->{rownum} = $rownum;
                    $record->{filenum} = $filenum;
                    $record->{filename} = $file;

                    my %r = %$record; my @row_ext = @r{@NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::fieldnames};
                    if ($context->{upsert}) {
                        push(@row_ext,$record->{cc},$record->{ac},$record->{sn});
                    } else {
                        push(@row_ext,$NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::added_delta);
                    }
                    push(@subscriber_rows,\@row_ext); # if &{$context->{check_number_code}}($context,$record);

                    #my %r = %$record;
                    #$record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::contact_fieldnames}]);
                    #next unless _unfold_number_ranges($context,$record,\@subscriber_rows);
                    if ($subscriber_import_single_row_txn and (scalar @subscriber_rows) > 0) {
                        while (defined (my $subscriber_row = shift @subscriber_rows)) {
                            if ($skip_errors) {
                                eval { _insert_subscriber_rows($context,[$subscriber_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_subscriber_rows($context,[$subscriber_row]);
                            }
                        }
                    }
                }

                if (not $subscriber_import_single_row_txn and (scalar @subscriber_rows) > 0) {
                    if ($skip_errors) {
                        eval { _insert_subscriber_rows($context,\@subscriber_rows); };
                        _warn($context,$@) if $@;
                    } else {
                        _insert_subscriber_rows($context,\@subscriber_rows);
                    }
                }
                #use Data::Dumper;
                #print Dumper(\@subscriber_rows);
                return 1;
            },
            init_process_context_code => sub {
                my ($context)= @_;
                $context->{db} = &get_import_db(); # keep ref count low..
                $context->{upsert} = $upsert;
                #$context->{unfold_ranges} = $subscriber_import_unfold_ranges;
                #$context->{fieldnames} = \@NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::fieldnames;
                #$context->{added_delta} = $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::added_delta;
                #$context->{create_new_record_code} = sub {
                #    return NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber->new(shift);
                #};

                #$context->{check_number_code} = sub {
                #    my ($context,$record) = @_;
                #    my $result = 1;
                #    my $number = $record->{dn};
                #    my $number = $record->{cc} . $record->{ac} . $record->{sn};
                #    if (NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                #        if ($skip_errors) {
                #            _warn($context,"$record->{sip_username}: duplicate number $number");
                #        } else {
                #            _error($context,"$record->{sip_username}: duplicate number $number");
                #        }
                #        $result = 0;
                #    }
                #    return $result;
                #};
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
        );
    }

    return ($result,$warning_count);

}

sub _import_subscriber_checks {
    my ($file) = @_;
    my $result = 1;

    return $result;
}

sub _import_subscriber_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::countby_ccacsn() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::update_delta(undef,undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::deleted_delta) .
            ' subscriber records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_subscriber_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::getinsertstatement($ignore_subscriber_unique)),
        #NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::Subscriber::gettablename(),
        #lock
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

sub _split_dn {
    my ($dn) = @_;
    my ($cc,$ac,$sn) = ('','',$dn);
    if ($cc_ac_map) {
        if ($default_cc) {
            $cc = $default_cc;
            $dn =~ s/^0//;
            $sn = $dn;
        } else {
            foreach my $cc_length ($cc_len_min .. $cc_len_max) {
                my ($_cc,$_dn) = (substr($dn,0,$cc_length), substr($dn,$cc_length));
                if (exists $cc_ac_map->{$_cc}) {
                    $cc = $_cc;
                    $sn = $_dn;
                    $dn = $_dn;
                    last;
                }
            }
        }
        if (exists $cc_ac_map->{$cc}) {
            my $ac_map = $cc_ac_map->{$cc};
            foreach my $ac_length ($ac_len->{$cc}->{min} .. $ac_len->{$cc}->{max}) {
                my ($_ac,$_sn) = (substr($dn,0,$ac_length), substr($dn,$ac_length));
                if (exists $ac_map->{$_ac}) {
                    $ac = $_ac;
                    $sn = $_sn;
                    #$dn = '';
                    last;
                }
            }
        }
    }
    return ($cc,$ac,$sn);

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
