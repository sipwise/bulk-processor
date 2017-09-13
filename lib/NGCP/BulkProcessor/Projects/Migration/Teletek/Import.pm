package NGCP::BulkProcessor::Projects::Migration::Teletek::Import;
use strict;

## no critic

use threads::shared qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Settings qw(
    $import_multithreading
    $subscriber_import_numofthreads
    $ignore_subscriber_unique
    $subscriber_import_single_row_txn
    $subscriber_import_unfold_ranges

    $allowedcli_import_numofthreads
    $ignore_allowedcli_unique
    $allowedcli_import_single_row_txn

    $clir_import_numofthreads
    $ignore_clir_unique
    $clir_import_single_row_txn

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

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli qw();
use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(threadid zerofill trim);
use NGCP::BulkProcessor::Table qw(get_rowhash);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_subscriber
    import_allowedcli
    import_clir
);

sub import_subscriber {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::create_table(0);

    foreach my $file (@files) {
        $result &= _import_subscriber_checks($file);
    }

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($subscriber_import_numofthreads);

    my $upsert = _import_subscriber_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    foreach my $file (@files) {
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @subscriber_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber->new($row);
                    $record->{cc} //= '';
                    $record->{ac} //= '';
                    $record->{sn} //= '';
                    $record->{rownum} = $rownum;
                    $record->{filename} = $file;
                    my @subscriber_row;
                    my %r;
                    if ($subscriber_import_unfold_ranges and $record->{sn} =~ /\.+$/) {
                        #if ($record->{sn} == '2861..') {
                        #print "x";
                        #}
                        my $pow = scalar (() = $record->{sn} =~ /\./g);
                        _warn($context,"number range $record->{sn} results in " . 10**$pow . ' numbers') if $pow > 2;
                        $record->{sn} =~ s/\.+$//g;
                        $record->{range} = 0;
                        %r = %$record; $record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::contact_fieldnames}]);
                        my $base_sn = $record->{sn};
                        %r = %$record; @subscriber_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::fieldnames};
                        if ($context->{upsert}) {
                            push(@subscriber_row,$record->{cc},$record->{ac},$record->{sn});
                        } else {
                            push(@subscriber_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::added_delta);
                        }
                        push(@subscriber_rows, [@subscriber_row]);
                        for (my $i = 0; $i < 10**$pow; $i++) {
                            $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber->new($record);
                            #@subscriber_row = @$row;
                            $record->{sn} = $base_sn . zerofill($i,$pow);
                            $record->{range} = 1;
                            %r = %$record; @subscriber_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::fieldnames};
                            if ($context->{upsert}) {
                                push(@subscriber_row,$record->{cc},$record->{ac},$record->{sn});
                            } else {
                                push(@subscriber_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::added_delta);
                            }
                            push(@subscriber_rows,[@subscriber_row]);
                        }
                        #if ($base_sn == '2861') {
                        #print "x";
                        #last;
                        #}

                    } else {
                        $record->{range} = 0;
                        %r = %$record; $record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::contact_fieldnames}]);
                        %r = %$record; @subscriber_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::fieldnames};
                        if ($context->{upsert}) {
                            push(@subscriber_row,$record->{cc},$record->{ac},$record->{sn});
                        } else {
                            push(@subscriber_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::added_delta);
                        }
                        push(@subscriber_rows,\@subscriber_row);
                    }
                }

                if ((scalar @subscriber_rows) > 0) {
                    if ($subscriber_import_single_row_txn) {
                        foreach my $subscriber_row (@subscriber_rows) {
                            if ($skip_errors) {
                                eval { _insert_subscriber_rows($context,[$subscriber_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_subscriber_rows($context,[$subscriber_row]);
                            }
                        }
                    } else {
                        if ($skip_errors) {
                            eval { _insert_subscriber_rows($context,\@subscriber_rows); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_subscriber_rows($context,\@subscriber_rows);
                        }
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

sub _import_subscriber_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_ccacsn() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::update_delta(undef,undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::deleted_delta) .
            ' subscriber records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_subscriber_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::getinsertstatement($ignore_subscriber_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
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

#sub _insert_subscriber_row {
#    my ($context,$subscriber_row) = @_;
#    $context->{db}->db_do(
#        ($context->{upsert} ?
#            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::getupsertstatement()
#            : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::getinsertstatement($ignore_subscriber_unique)),
#        @$subscriber_row
#    );
#}







sub import_allowedcli {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::create_table(0);

    foreach my $file (@files) {
        $result &= _import_allowedcli_checks($file);
    }

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($allowedcli_import_numofthreads);

    my $upsert = _import_allowedcli_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    foreach my $file (@files) {
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @allowedcli_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli->new($row);
                    $record->{cc} //= '';
                    $record->{ac} //= '';
                    $record->{sn} //= '';
                    $record->{rownum} = $rownum;
                    $record->{filename} = $file;

                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        my $number = $record->{cc} . $record->{ac} . $record->{sn};
                        if ($skip_errors) {
                            _warn($context,"duplicate number: $number");
                        } else {
                            _error($context,"duplicate number: $number");
                        }
                        next;
                    }

                    if ((scalar @{NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_sipusername($record->{sip_username})}) == 0) {
                        if ($skip_errors) {
                            _warn($context,"sip username $record->{sip_username} not found");
                        } else {
                            _error($context,"sip username $record->{sip_username} not found");
                        }
                        next;
                    }

                    my %r = %$record; my @allowedcli_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::fieldnames};
                    if ($context->{upsert}) {
                        push(@allowedcli_row,$record->{cc},$record->{ac},$record->{sn});
                    } else {
                        push(@allowedcli_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::added_delta);
                    }
                    push(@allowedcli_rows,\@allowedcli_row);
                }

                if ((scalar @allowedcli_rows) > 0) {
                    if ($allowedcli_import_single_row_txn) {
                        foreach my $allowedcli_row (@allowedcli_rows) {
                            if ($skip_errors) {
                                eval { _insert_allowedcli_rows($context,[$allowedcli_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_allowedcli_rows($context,[$allowedcli_row]);
                            }
                        }
                    } else {
                        if ($skip_errors) {
                            eval { _insert_allowedcli_rows($context,\@allowedcli_rows); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_allowedcli_rows($context,\@allowedcli_rows);
                        }
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

sub _import_allowedcli_checks {
    my ($file) = @_;
    my $result = 1;
    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_ccacsn();
    };
    if ($@ or $subscribercount == 0) {
        fileprocessingerror($file,'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
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

sub _import_allowedcli_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_ccacsn() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::update_delta(undef,undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::deleted_delta) .
            ' allowed cli records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_allowedcli_rows {
    my ($context,$allowedcli_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::getinsertstatement($ignore_allowedcli_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
        #lock
    );
    eval {
        $context->{db}->db_do_rowblock($allowedcli_rows);
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









sub import_clir {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::create_table(0);

    foreach my $file (@files) {
        $result &= _import_clir_checks($file);
    }

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($clir_import_numofthreads);

    my $upsert = _import_clir_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    foreach my $file (@files) {
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @clir_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir->new($row);
                    $record->{rownum} = $rownum;
                    $record->{filename} = $file;

                    if ((scalar @{NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_sipusername($record->{sip_username})}) == 0) {
                        if ($skip_errors) {
                            _warn($context,"sip username $record->{sip_username} not found");
                        } else {
                            _error($context,"sip username $record->{sip_username} not found");
                        }
                        next;
                    }

                    my %r = %$record; my @clir_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::fieldnames};
                    if ($context->{upsert}) {
                        push(@clir_row,$record->{sip_username});
                    } else {
                        push(@clir_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::added_delta);
                    }
                    push(@clir_rows,\@clir_row);
                }

                if ((scalar @clir_rows) > 0) {
                    if ($clir_import_single_row_txn) {
                        foreach my $clir_row (@clir_rows) {
                            if ($skip_errors) {
                                eval { _insert_clir_rows($context,[$clir_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_clir_rows($context,[$clir_row]);
                            }
                        }
                    } else {
                        if ($skip_errors) {
                            eval { _insert_clir_rows($context,\@clir_rows); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_clir_rows($context,\@clir_rows);
                        }
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

sub _import_clir_checks {
    my ($file) = @_;
    my $result = 1;
    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::countby_ccacsn();
    };
    if ($@ or $subscribercount == 0) {
        fileprocessingerror($file,'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    #my $allowedclicount = 0;
    #eval {
    #    $allowedclicount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_ccacsn();
    #};
    #if ($@ or $allowedclicount == 0) {
    #    fileprocessingerror($file,'please import allowed clis first',getlogger(__PACKAGE__));
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

sub _import_clir_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::countby_clir() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::deleted_delta) .
            ' clir records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_clir_rows {
    my ($context,$clir_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::getinsertstatement($ignore_clir_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
        #lock
    );
    eval {
        $context->{db}->db_do_rowblock($clir_rows);
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
