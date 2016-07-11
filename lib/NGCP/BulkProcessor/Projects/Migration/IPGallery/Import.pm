package NGCP::BulkProcessor::Projects::Migration::IPGallery::Import;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use NGCP::BulkProcessor::Globals qw(
    $cpucount
);
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $import_multithreading
    $features_define_import_numofthreads
    $skip_duplicate_setoptionitems
    $ignore_options_unique
    $ignore_setoptionitems_unique
    $subscriber_define_import_numofthreads
    $subscribernumer_exclude_pattern
    $subscribernumer_exclude_exception_pattern
    $ignore_subscriber_unique
    $skip_prepaid_subscribers
    $lnp_define_import_numofthreads
    $ignore_lnp_unique
    $user_password_import_numofthreads
    $ignore_user_password_unique
    $batch_import_numofthreads
    $ignore_batch_unique
    $dry
);
use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingwarn
    fileprocessingerror
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::SubscriberDefineFile qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::LnpDefineFile qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::UserPasswordFile qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::BatchFile qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
    import_user_password
    import_batch
);

sub import_features_define {

    my ($file) = @_;
    # create tables:
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::create_table(0);
    $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::create_table(0);

    # checks, e.g. other table must be present:
    # ..none..

    # prepare parse:
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile->new($features_define_import_numofthreads);
    $importer->stoponparseerrors(!$dry);

    my $upsert = _import_features_define_reset_delta();

    # launch:
    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @featureoption_rows = ();
            my @featureoptionsetitem_rows = ();
            foreach my $line (@$rows) {
                $rownum++;
                my $row = undef;
                if (not $importer->parselines()) {
                    eval {
                        $row = NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser::parse(\$line,$context->{grammar});
                    };
                    if ($@) {
                        if ($importer->stoponparseerrors()) {
                            fileprocessingerror($context->{filename},'record ' . $rownum . ' - ' . $@,getlogger(__PACKAGE__));
                        } else {
                            fileprocessingwarn($context->{filename},'record ' . $rownum . ' - ' . $@,getlogger(__PACKAGE__));
                        }
                    }
                }
                next unless defined $row;
                foreach my $subscriber_number (keys %$row) {
                    foreach my $option (@{$row->{$subscriber_number}}) {
                        if ('HASH' eq ref $option) {
                            foreach my $setoption (keys %$option) {
                                foreach my $setoptionitem (@{$skip_duplicate_setoptionitems ? removeduplicates($option->{$setoption}) : $option->{$setoption}}) {
                                    if ($context->{upsert}) {
                                        push(@featureoptionsetitem_rows,[ $subscriber_number, $setoption, $setoptionitem,
                                            $subscriber_number, $setoption, $setoptionitem ]);
                                    } else {
                                        push(@featureoptionsetitem_rows,[ $subscriber_number, $setoption, $setoptionitem,
                                            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::added_delta ]);
                                    }
                                }
                                if ($context->{upsert}) {
                                    push(@featureoption_rows,[ $subscriber_number, $setoption,
                                        $subscriber_number, $setoption ]);
                                } else {
                                    push(@featureoption_rows,[ $subscriber_number, $setoption,
                                        $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::added_delta ]);
                                }
                            }
                        } else {
                            if ($context->{upsert}) {
                                push(@featureoption_rows,[ $subscriber_number, $option,
                                    $subscriber_number, $option ]);
                            } else {
                                push(@featureoption_rows,[ $subscriber_number, $option,
                                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::added_delta ]);
                            }
                        }
                    }
                }
            }

            if ((scalar @featureoption_rows) > 0) {
                if ($dry) {
                    eval { _insert_featureoption_rows($context,\@featureoption_rows); };
                } else {
                    _insert_featureoption_rows($context,\@featureoption_rows);
                }
            }
            if ((scalar @featureoptionsetitem_rows) > 0) {
                if ($dry) {
                    eval { _insert_featureoptionsetitem_rows($context,\@featureoptionsetitem_rows); };
                } else {
                    _insert_featureoptionsetitem_rows($context,\@featureoptionsetitem_rows);
                }
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            if (not $importer->parselines()) {
                eval {
                    $context->{grammar} = NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser::create_grammar();
                };
                if ($@) {
                    fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
                }
            }
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _import_features_define_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::update_delta(undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::deleted_delta) .
            ' feature option records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::countby_subscribernumber_option_optionsetitem() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::update_delta(undef,undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::deleted_delta) .
            ' feature set option item records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_featureoption_rows {
    my ($context,$featureoption_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::added_delta
         ) : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::getinsertstatement($ignore_options_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::gettablename(),
        #lock - $import_multithreading
    );
    $context->{db}->db_do_rowblock($featureoption_rows);
    $context->{db}->db_finish();
}

sub _insert_featureoptionsetitem_rows {
    my ($context,$featureoptionsetitem_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::added_delta
         ) : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::getinsertstatement($ignore_setoptionitems_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($featureoptionsetitem_rows);
    $context->{db}->db_finish();
}

sub import_subscriber_define {

    my ($file) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::create_table(0);

    $result &= _import_subscriber_define_checks($file);

    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::SubscriberDefineFile->new($subscriber_define_import_numofthreads);

    my $upsert = _import_subscriber_define_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @subscriber_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                my $record = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber->new($row);
                next if 'None' eq $record->{rgw_fqdn};
                if ($record->{dial_number} =~ $subscribernumer_exclude_pattern) {
                    if ($record->{dial_number} =~ $subscribernumer_exclude_exception_pattern) {
                        processing_info($context->{tid},'record ' . $rownum . ' - exclude exception pattern match: ' . $record->{dial_number},getlogger(__PACKAGE__));
                        next unless _import_subscriber_define_referential_checks($context,$record,$rownum);
                    } else {
                        processing_info($context->{tid},'record ' . $rownum . ' - skipped, exclude pattern match: ' . $record->{dial_number},getlogger(__PACKAGE__));
                        next;
                    }
                } else {
                    next unless _import_subscriber_define_referential_checks($context,$record,$rownum);
                }
                my @subscriber_row = @$row;
                if ($context->{upsert}) {
                    push(@subscriber_row,NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::split_subscribernumber($record->subscribernumber()));
                } else {
                    push(@subscriber_row,$NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::added_delta);
                }
                push(@subscriber_rows,\@subscriber_row);
            }

            if ((scalar @subscriber_rows) > 0) {
                if ($dry) {
                    eval { _insert_subscriber_rows($context,\@subscriber_rows); };
                } else {
                    _insert_subscriber_rows($context,\@subscriber_rows);
                }
            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _import_subscriber_define_referential_checks {
    my ($context,$record,$rownum) = @_;
    my $result = 1;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option($record->subscribernumber()) > 0) {
        if ($skip_prepaid_subscribers) {
            my $prepaid_option_set_item = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::findby_subscribernumber_option_optionsetitem(
                    $record->subscribernumber(),
                    $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::PRE_PAID_SERVICE_OPTION_SET,
                )->[0];
            if (defined $prepaid_option_set_item and $prepaid_option_set_item->{delta} ne
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::deleted_delta) {
                processing_info($context->{tid},'record ' . $rownum . ' - skipped, ' . $prepaid_option_set_item->{optionsetitem} . ': ' . $record->{dial_number},getlogger(__PACKAGE__));
                $result &= 0;
            }
        }
    } else {
        $result &= 0;
        if ($dry) {
            fileprocessingwarn($context->{filename},'record ' . $rownum . ' - no features records for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        } else {
            fileprocessingerror($context->{filename},'record ' . $rownum . ' - no features records for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        }
    }

    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn($record->subscribernumber()) > 0) {

    } else {
        $result &= 0;
        if ($dry) {
            fileprocessingwarn($context->{filename},'record ' . $rownum . ' - no username password record for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        } else {
            fileprocessingerror($context->{filename},'record ' . $rownum . ' - no username password record for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        }
    }

    return $result;

}

sub _import_subscriber_define_checks {
    my ($file) = @_;
    my $result = 1;
    my $optioncount = 0;
    eval {
        $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option();
    };
    if ($@ or $optioncount == 0) {
        fileprocessingerror($file,'please import subscriber features first',getlogger(__PACKAGE__));
        $result = 0; #even in dry mode..
    }
    my $userpasswordcount = 0;
    eval {
        $userpasswordcount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn();
    };
    if ($@ or $userpasswordcount == 0) {
        fileprocessingerror($file,'please import user passwords first',getlogger(__PACKAGE__));
        $result = 0; #even in dry mode..
    }
    return $result;
}

sub _import_subscriber_define_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) .
            ' subscriber records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_subscriber_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::added_delta
         )
         : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::getinsertstatement($ignore_subscriber_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($subscriber_rows);
    $context->{db}->db_finish();
}

sub import_lnp_define {

    my ($file) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::create_table(0);

    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::LnpDefineFile->new($lnp_define_import_numofthreads);

    my $upsert = _import_lnp_define_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @lnp_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                my @lnp_row = @$row;
                shift @lnp_row; #ignore first col
                my $record = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp->new(\@lnp_row);
                next if $record->{type} eq $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::IN_TYPE;
                $record->{lrn_code} = substr($record->{lrn_code},0,4);
                @lnp_row = ( $record->{ported_number}, $record->{type}, $record->{lrn_code} );
                if ($context->{upsert}) {
                    push(@lnp_row,$record->{lrn_code}, $record->{ported_number});
                } else {
                    push(@lnp_row,$NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::added_delta);
                }
                push(@lnp_rows,\@lnp_row);
            }

            if ((scalar @lnp_rows) > 0) {
                if ($dry) {
                    eval { _insert_lnp_rows($context,\@lnp_rows); };
                } else {
                    _insert_lnp_rows($context,\@lnp_rows);
                }
            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _import_lnp_define_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::countby_lrncode_portednumber() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::update_delta(undef,undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::deleted_delta) .
            ' lnp number records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_lnp_rows {
    my ($context,$lnp_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::added_delta
         )
         : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::getinsertstatement($ignore_lnp_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($lnp_rows);
    $context->{db}->db_finish();
}


sub import_user_password {

    my ($file) = @_;
    # create tables:
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::create_table(0);

    # checks, e.g. other table must be present:
    # ..none..

    # prepare parse:
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::UserPasswordFile->new($user_password_import_numofthreads);

    my $upsert = _import_user_password_reset_delta();

    # launch:
    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @usernamepassword_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                my @usernamepassword_row = @$row;
                my $record = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword->new(\@usernamepassword_row);
                if ($context->{upsert}) {
                    push(@usernamepassword_row,$record->{fqdn});
                } else {
                    push(@usernamepassword_row,$NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::added_delta);
                }
                push(@usernamepassword_rows,\@usernamepassword_row);
            }

            if ((scalar @usernamepassword_rows) > 0) {
                if ($dry) {
                    eval { _insert_usernamepassword_rows($context,\@usernamepassword_rows); };
                } else {
                    _insert_usernamepassword_rows($context,\@usernamepassword_rows);
                }
            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _import_user_password_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::countby_fqdn() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::deleted_delta) .
            ' username password records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_usernamepassword_rows {
    my ($context,$usernamepassword_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::added_delta
         ) : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::getinsertstatement($ignore_user_password_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::gettablename(),
        #lock - $import_multithreading
    );
    $context->{db}->db_do_rowblock($usernamepassword_rows);
    $context->{db}->db_finish();
}

sub import_batch {

    my ($file) = @_;

    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::create_table(0);

    $result &= _import_batch_checks($file);

    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::BatchFile->new($batch_import_numofthreads);

    my $upsert = _import_batch_reset_delta();

    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @batch_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                my $record = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch->new($row);
                next unless _import_batch_referential_checks($context,$record,$rownum);
                my @batch_row = @$row;
                if ($context->{upsert}) {
                    push(@batch_row,$record->{number});
                } else {
                    push(@batch_row,$NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::added_delta);
                }
                push(@batch_rows,\@batch_row);
            }

            if ((scalar @batch_rows) > 0) {
                if ($dry) {
                    eval { _insert_batch_rows($context,\@batch_rows); };
                } else {
                    _insert_batch_rows($context,\@batch_rows);
                }
            }

            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
            $context->{upsert} = $upsert;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _import_batch_referential_checks {
    my ($context,$record,$rownum) = @_;
    my $result = 1;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber($record->{number}) > 0) {

    } else {
        $result &= 0;
        if ($dry) {
            fileprocessingwarn($context->{filename},'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number},getlogger(__PACKAGE__));
        } else {
            fileprocessingerror($context->{filename},'record ' . $rownum . ' - no subscriber record for batch number found: ' . $record->{number},getlogger(__PACKAGE__));
        }
    }

    return $result;

}

sub _import_batch_checks {
    my ($file) = @_;
    my $result = 1;
    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
    };
    if ($@ or $subscribercount == 0) {
        fileprocessingerror($file,'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in dry mode..
    }
    return $result;
}

sub _import_batch_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::countby_number() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::deleted_delta) .
            ' batch records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_batch_rows {
    my ($context,$batch_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
         NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::getupsertstatement(
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::updated_delta,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::added_delta
         )
         : NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Batch::getinsertstatement($ignore_batch_unique)),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($batch_rows);
    $context->{db}->db_finish();
}

1;
