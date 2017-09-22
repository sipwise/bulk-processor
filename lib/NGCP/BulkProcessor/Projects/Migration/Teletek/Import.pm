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
    $allowedcli_import_unfold_ranges

    $clir_import_numofthreads
    $ignore_clir_unique
    $clir_import_single_row_txn

    $callforward_import_numofthreads
    $ignore_callforward_unique
    $callforward_import_single_row_txn

    $registration_import_numofthreads
    $ignore_registration_unique
    $registration_import_single_row_txn

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
    import_callforward
    import_registration
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
                    my %r = %$record;
                    $record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::contact_fieldnames}]);
                    next unless _unfold_number_ranges($context,$record,\@subscriber_rows);
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
                $context->{unfold_ranges} = $subscriber_import_unfold_ranges;
                $context->{fieldnames} = \@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::fieldnames;
                $context->{added_delta} = $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::added_delta;
                $context->{create_new_record_code} = sub {
                    return NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber->new(shift);
                };
                $context->{check_number_code} = sub {
                    my ($context,$record) = @_;
                    my $result = 1;
                    my $number = $record->{cc} . $record->{ac} . $record->{sn};
                    # prevent db's unique constraint violation:
                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        if ($skip_errors) {
                            _warn($context,"$record->{sip_username}: duplicate number $number");
                        } else {
                            _error($context,"$record->{sip_username}: duplicate number $number");
                        }
                        $result = 0;
                    }
                    return $result;
                };
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

                    if ((scalar @{NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_sipusername($record->{sip_username})}) == 0) {
                        my $number = $record->{cc} . $record->{ac} . $record->{sn};
                        if ($skip_errors) {
                            _warn($context,"$number: sip username $record->{sip_username} not found");
                        } else {
                            _error($context,"$number: sip username $record->{sip_username} not found");
                        }
                        next;
                    }

                    next unless _unfold_number_ranges($context,$record,\@allowedcli_rows);
                    if ($allowedcli_import_single_row_txn and (scalar @allowedcli_rows) > 0) {
                        while (defined (my $allowedcli_row = shift @allowedcli_rows)) {
                            if ($skip_errors) {
                                eval { _insert_allowedcli_rows($context,[$allowedcli_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_allowedcli_rows($context,[$allowedcli_row]);
                            }
                        }
                    }
                }

                if (not $allowedcli_import_single_row_txn and (scalar @allowedcli_rows) > 0) {
                    if ($skip_errors) {
                        eval { _insert_allowedcli_rows($context,\@allowedcli_rows); };
                        _warn($context,$@) if $@;
                    } else {
                        _insert_allowedcli_rows($context,\@allowedcli_rows);
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
                $context->{unfold_ranges} = $allowedcli_import_unfold_ranges;
                $context->{fieldnames} = \@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::fieldnames;
                $context->{added_delta} = $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::added_delta;
                $context->{create_new_record_code} = sub {
                    return NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli->new(shift);
                };
                $context->{check_number_code} = sub {
                    my ($context,$record) = @_;
                    my $result = 1;
                    my $number = $record->{cc} . $record->{ac} . $record->{sn};
                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        if ($skip_errors) {
                            _warn($context,"$record->{sip_username}: duplicate number $number");
                        } else {
                            _error($context,"$record->{sip_username}: duplicate number $number");
                        }
                        $result = 0;
                    }
                    # prevent db's unique constraint violation:
                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        if ($skip_errors) {
                            _warn($context,"$record->{sip_username}: duplicate number $number");
                        } else {
                            _error($context,"$record->{sip_username}: duplicate number $number");
                        }
                        $result = 0;
                    }
                    return $result;
                };
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

sub _unfold_number_ranges {

    my ($context,$record,$rows) = @_;

    sub create_new_record_code{}

    my $result = 0;
    my @fieldnames = @{$context->{fieldnames}};
    my $cc_ac_ok = ($record->{cc} =~ /^\d*$/ and $record->{ac} =~ /^\d*$/);
    my @row;
    my %r;
    if ($context->{unfold_ranges} and $cc_ac_ok and $record->{sn} =~ /\.+$/) {
        #if ($record->{sn} == '2861..') {
        #print "x";
        #}
        my $pow = scalar (() = $record->{sn} =~ /\./g);
        _info($context,"expanding number range '$record->{sn}' to " . 10**$pow . ' numbers');
        _warn($context,"expanding number range '$record->{sn}' results in " . 10**$pow . ' numbers') if $pow > 2;
        $record->{sn} =~ s/\.+$//g;
        $record->{range} = 0;
        #%r = %$record; $record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::contact_fieldnames}]);
        my $base_sn = $record->{sn};
        %r = %$record; @row = @r{@fieldnames};
        if ($context->{upsert}) {
            push(@row,$record->{cc},$record->{ac},$record->{sn});
        } else {
            push(@row,$context->{added_delta});
        }
        push(@$rows, [@row]) if &{$context->{check_number_code}}($context,$record);
        for (my $i = 0; $i < 10**$pow; $i++) {
            $record = &{$context->{create_new_record_code}}($record);
            #@subscriber_row = @$row;
            $record->{sn} = $base_sn . zerofill($i,$pow);
            $record->{range} = 1;
            %r = %$record; @row = @r{@fieldnames};
            if ($context->{upsert}) {
                push(@row,$record->{cc},$record->{ac},$record->{sn});
            } else {
                push(@row,$context->{added_delta});
            }
            push(@$rows,[@row]) if &{$context->{check_number_code}}($context,$record);
        }
        #if ($base_sn == '2861') {
        #print "x";
        #last;
        #}
        $result = 1;
    } elsif ($cc_ac_ok and $record->{sn} =~ /^\d*$/) {
        $record->{range} = 0;
        #%r = %$record; $record->{contact_hash} = get_rowhash([@r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::contact_fieldnames}]);
        %r = %$record; @row = @r{@fieldnames};
        if ($context->{upsert}) {
            push(@row,$record->{cc},$record->{ac},$record->{sn});
        } else {
            push(@row,$context->{added_delta});
        }
        push(@$rows,\@row) if &{$context->{check_number_code}}($context,$record);
        $result = 1;
    } else {
        my $number = $record->{cc} . $record->{ac} . $record->{sn};
        if ($skip_errors) {
            _warn($context,"invalid number: $number");
        } else {
            _error($context,"invalid number: $number");
        }
        $result = 0;
    }
    return $result;
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
                    # prevent db's unique constraint violation:
                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Clir::findby_sipusername($record->{sip_username})) {
                        if ($skip_errors) {
                            _warn($context,"duplicate sip username $record->{sip_username}");
                        } else {
                            _error($context,"duplicate sip username $record->{sip_username}");
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
                    if ($clir_import_single_row_txn and (scalar @clir_rows) > 0) {
                        while (defined (my $clir_row = shift @clir_rows)) {
                            if ($skip_errors) {
                                eval { _insert_clir_rows($context,[$clir_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_clir_rows($context,[$clir_row]);
                            }
                        }
                    }
                }

                if (not $clir_import_single_row_txn and (scalar @clir_rows) > 0) {
                    if ($skip_errors) {
                        eval { _insert_clir_rows($context,\@clir_rows); };
                        _warn($context,$@) if $@;
                    } else {
                        _insert_clir_rows($context,\@clir_rows);
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

sub import_callforward {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::create_table(0);

    foreach my $file (@files) {
        $result &= _import_callforward_checks($file);
    }

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($callforward_import_numofthreads);

    my $upsert = _import_callforward_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    foreach my $file (@files) {
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @callforward_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward->new($row);
                    $record->{cc} //= '';
                    $record->{ac} //= '';
                    $record->{sn} //= '';
                    $record->{rownum} = $rownum;
                    $record->{filename} = $file;

                    if (my $subscriber = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        $record->{sip_username} = $subscriber->{sip_username};
                    } elsif (my $allowedcli = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::findby_ccacsn($record->{cc},$record->{ac},$record->{sn})) {
                        $record->{sip_username} = $subscriber->{sip_username};
                    } else {
                        my $number = $record->{cc} . $record->{ac} . $record->{sn};
                        if ($skip_errors) {
                            _warn($context,"no sip username found for number $number");
                        } else {
                            _error($context,"no sip username found for number $number");
                        }
                        next;
                    }

                    my %r = %$record; my @callforward_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::fieldnames};
                    if ($context->{upsert}) {
                        push(@callforward_row,$record->{cc},$record->{ac},$record->{sn},$record->{type},$record->{destination});
                    } else {
                        push(@callforward_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::added_delta);
                    }
                    push(@callforward_rows,\@callforward_row);
                    if ($callforward_import_single_row_txn and (scalar @callforward_rows) > 0) {
                        while (defined (my $callforward_row = shift @callforward_rows)) {
                            if ($skip_errors) {
                                eval { _insert_callforward_rows($context,[$callforward_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_callforward_rows($context,[$callforward_row]);
                            }
                        }
                    }
                }

                if (not $callforward_import_single_row_txn and (scalar @callforward_rows) > 0) {
                    if ($skip_errors) {
                        eval { _insert_callforward_rows($context,\@callforward_rows); };
                        _warn($context,$@) if $@;
                    } else {
                        _insert_callforward_rows($context,\@callforward_rows);
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

sub _import_callforward_checks {
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
    my $allowedclicount = 0;
    eval {
        $allowedclicount = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::AllowedCli::countby_ccacsn();
    };
    if ($@ or $allowedclicount == 0) {
        fileprocessingerror($file,'please import allowedclis first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}

sub _import_callforward_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::countby_ccacsntype() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::deleted_delta) .
            ' callforward records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_callforward_rows {
    my ($context,$callforward_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward::getinsertstatement($ignore_callforward_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
        #lock
    );
    eval {
        $context->{db}->db_do_rowblock($callforward_rows);
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










sub import_registration {

    my (@files) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::create_table(0);

    foreach my $file (@files) {
        $result &= _import_registration_checks($file);
    }

    my $importer = NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile->new($registration_import_numofthreads);

    my $upsert = _import_registration_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    my $warning_count :shared = 0;
    foreach my $file (@files) {
        $result &= $importer->process(
            file => $file,
            process_code => sub {
                my ($context,$rows,$row_offset) = @_;
                my $rownum = $row_offset;
                my @registration_rows = ();
                foreach my $row (@$rows) {
                    $rownum++;
                    next if (scalar @$row) == 0;
                    $row = [ map { local $_ = $_; trim($_); } @$row ];
                    my $record = NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration->new($row);
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
                    # prevent db's unique constraint violation:
                    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::findby_sipusername($record->{sip_username})) {
                        if ($skip_errors) {
                            _warn($context,"duplicate sip username $record->{sip_username}");
                        } else {
                            _error($context,"duplicate sip username $record->{sip_username}");
                        }
                        next;
                    }

                    my %r = %$record; my @registration_row = @r{@NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::fieldnames};
                    if ($context->{upsert}) {
                        push(@registration_row,$record->{sip_username});
                    } else {
                        push(@registration_row,$NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::added_delta);
                    }
                    push(@registration_rows,\@registration_row);
                    if ($registration_import_single_row_txn and (scalar @registration_rows) > 0) {
                        while (defined (my $registration_row = shift @registration_rows)) {
                            if ($skip_errors) {
                                eval { _insert_registration_rows($context,[$registration_row]); };
                                _warn($context,$@) if $@;
                            } else {
                                _insert_registration_rows($context,[$registration_row]);
                            }
                        }
                    }
                }

                if (not $registration_import_single_row_txn and (scalar @registration_rows) > 0) {
                    if ($skip_errors) {
                        eval { _insert_registration_rows($context,\@registration_rows); };
                        _warn($context,$@) if $@;
                    } else {
                        _insert_registration_rows($context,\@registration_rows);
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

sub _import_registration_checks {
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

    return $result;
}

sub _import_registration_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::countby_sipcontact() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::deleted_delta) .
            ' registration records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_registration_rows {
    my ($context,$registration_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::getupsertstatement()
         : NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Registration::getinsertstatement($ignore_registration_unique)),
        #NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber::gettablename(),
        #lock
    );
    eval {
        $context->{db}->db_do_rowblock($registration_rows);
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
