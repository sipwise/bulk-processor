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
    $subscriber_define_import_numofthreads
    $lnp_define_import_numofthreads
    $dry
);
use NGCP::BulkProcessor::Logging qw (
    getlogger
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
    destroy_dbs
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSet qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Subscriber qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Lnp qw();

use NGCP::BulkProcessor::Array qw(removeduplicates);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_features_define
    import_subscriber_define
    import_lnp_define
);

sub import_features_define {

    my ($file) = @_;
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOption::create_table(1);
    $result &= NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSet::create_table(1);
    destroy_dbs(); #close all db connections before forking..
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile->new($features_define_import_numofthreads);
    $importer->stoponparseerrors(!$dry);
    return $result && $importer->process($file,sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @featureoption_rows = ();
            my @featureoptionset_rows = ();
            foreach my $line (@$rows) {
                my $row = undef;
                if (not $importer->parselines()) {
                    eval {
                        $row = NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser::parse(\$line,$context->{grammar});
                    };
                    if ($@) {
                        if ($importer->stoponparseerrors()) {
                            fileprocessingerror($context->{filename},'record ' . ($rownum + 1) . ' - ' . $@,getlogger(__PACKAGE__));
                        } else {
                            fileprocessingwarn($context->{filename},'record ' . ($rownum + 1) . ' - ' . $@,getlogger(__PACKAGE__));
                        }
                    }
                }
                next unless defined $row;
                $rownum++;
                foreach my $subscriber_number (keys %$row) {
                    foreach my $option (@{$row->{$subscriber_number}}) {
                        if ('HASH' eq ref $option) {
                            foreach my $setoption (keys %$option) {
                                foreach my $setoptionitem (@{$skip_duplicate_setoptionitems ? removeduplicates($option->{$setoption}) : $option->{$setoption}}) {
                                    push(@featureoptionset_rows,[ $subscriber_number, $setoption, $setoptionitem ]);
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
            if ((scalar @featureoptionset_rows) > 0) {
                if ($dry) {
                    eval { _insert_featureoptionset_rows($context,\@featureoptionset_rows); };
                } else {
                    _insert_featureoptionset_rows($context,\@featureoptionset_rows);
                }
            }
            return 1;
        }, sub {
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
        }, sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_dbs();
        },$import_multithreading);

}

sub _insert_featureoption_rows {
    my ($context,$featureoption_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOption::getinsertstatement(),
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOption::gettablename(),
        #lock - $import_multithreading
    );
    $context->{db}->db_do_rowblock($featureoption_rows);
    $context->{db}->db_finish();
}

sub _insert_featureoptionset_rows {
    my ($context,$featureoptionset_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSet::getinsertstatement(),
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::FeatureOptionSet::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($featureoptionset_rows);
    $context->{db}->db_finish();
}

sub import_subscriber_define {

    my ($file) = @_;
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Subscriber::create_table(1);
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::SubscriberDefineFile->new($subscriber_define_import_numofthreads);
    destroy_dbs(); #close all db connections before forking..
    return $result && $importer->process($file,sub {
            my ($context,$rows,$row_offset) = @_;

            if ((scalar @$rows) > 0) {
                if ($dry) {
                    eval { _insert_subscriber_rows($context,$rows); };
                } else {
                    _insert_subscriber_rows($context,$rows);
                }
            }

            return 1;
        }, sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
        }, sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_dbs();
        }, $import_multithreading);

}

sub _insert_subscriber_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Subscriber::getinsertstatement(),
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Subscriber::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($subscriber_rows);
    $context->{db}->db_finish();
}

sub import_lnp_define {

    my ($file) = @_;
    my $result = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Lnp::create_table(1);
    my $importer = NGCP::BulkProcessor::Projects::Migration::IPGallery::FileProcessors::LnpDefineFile->new($lnp_define_import_numofthreads);
    destroy_dbs(); #close all db connections before forking..
    return $result && $importer->process($file,sub {
            my ($context,$rows,$row_offset) = @_;

            my @lnp_rows = ();
            foreach my $row (@$rows) {
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
        }, sub {
            my ($context)= @_;
            $context->{db} = &get_import_db(); # keep ref count low..
        }, sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_dbs();
        }, $import_multithreading);

}

sub _insert_lnp_rows {
    my ($context,$lnp_rows) = @_;
    $context->{db}->db_do_begin(
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Lnp::getinsertstatement(),
        NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::Lnp::gettablename(),
        #lock
    );
    $context->{db}->db_do_rowblock($lnp_rows);
    $context->{db}->db_finish();
}

1;
