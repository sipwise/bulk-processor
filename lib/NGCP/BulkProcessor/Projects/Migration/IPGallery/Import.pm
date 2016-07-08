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
    $lnp_define_import_numofthreads
    $ignore_lnp_unique
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

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);
use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
);

sub import_features_define {

    my ($file) = @_;
    # create tables:
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::create_table(1);
    $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::create_table(1);

    # checks, e.g. other table must be present:
    # ..none..

    # prepare parse:
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile->new($features_define_import_numofthreads);
    $importer->stoponparseerrors(!$dry);

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
                                    push(@featureoptionsetitem_rows,[ $subscriber_number, $setoption, $setoptionitem ]);
                                }
                                push(@featureoption_rows,[ $subscriber_number, $setoption ]);
                            }
                        } else {
                            push(@featureoption_rows,[ $subscriber_number, $option ]);
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
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _insert_featureoption_rows {
    my ($context,$featureoption_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::getinsertstatement($ignore_options_unique),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::gettablename(),
        #lock - $import_multithreading
    );
    $context->{db}->db_do_rowblock($featureoption_rows);
    $context->{db}->db_finish();
}

sub _insert_featureoptionsetitem_rows {
    my ($context,$featureoptionsetitem_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOptionSetItem::getinsertstatement($ignore_setoptionitems_unique),
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
                    push(@subscriber_row,$record->{country_code},$record->{area_code},$record->{dial_number});
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
    my $result = 0;
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber($record->subscribernumber()) > 0) {
        $result = 1;
    } else {
        if ($dry) {
            fileprocessingwarn($context->{filename},'record ' . $rownum . ' - no features records for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        } else {
            fileprocessingerror($context->{filename},'record ' . $rownum . ' - no features records for subscriber found: ' . $record->{dial_number},getlogger(__PACKAGE__));
        }
    }
    return $result;

}

sub _import_subscriber_define_checks {
    my ($file) = @_;
    my $result = 1;
    my $optioncount = 0;
    eval {
        $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber();
    };
    if ($@ or $optioncount == 0) {
        fileprocessingerror($file,'please import subscriber features first',getlogger(__PACKAGE__));
        $result = 0; #even in dry mode..
    }
    return $result;
}

sub _import_subscriber_define_reset_delta {
    if (NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber() > 0) {
        processing_info(threadid(),'resetting subscriber delta of ' .
            NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::update_delta(undef,
            $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) .
            ' records',getlogger(__PACKAGE__));
        return 1;
    }
    return 0;
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

    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::create_table(1);

    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::LnpDefineFile->new($lnp_define_import_numofthreads);

    destroy_all_dbs(); #close all db connections before forking..
    return $result && $importer->process(
        file => $file,
        process_code => sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @lnp_rows = ();
            foreach my $row (@$rows) {
                $rownum++;
                next if $row->[2] eq 'In';
                $row->[3] = substr($row->[3],0,4);
                shift @$row; #ignore first col
                push(@lnp_rows,$row);
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
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        multithreading => $import_multithreading
    );

}

sub _insert_lnp_rows {
    my ($context,$lnp_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::getinsertstatement($ignore_lnp_unique),
        #NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Lnp::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($lnp_rows);
    $context->{db}->db_finish();
}

1;
