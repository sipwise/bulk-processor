package Projects::Migration::IPGallery::Import;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Globals qw(
    $cpucount
);
use Projects::Migration::IPGallery::Settings qw(
    $import_multithreading
    $feature_define_import_numofthreads
    $dry
);
use Logging qw (
    getlogger
);
use LogError qw(
    fileprocessingwarn
    fileprocessingerror
);

#use FileProcessors::CSVFile;
use Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile qw();
use Projects::Migration::IPGallery::FeaturesDefineParser qw();

use Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db
);

use Projects::Migration::IPGallery::Dao::FeatureOption qw();
use Projects::Migration::IPGallery::Dao::FeatureOptionSet qw();

use Array qw(removeduplicates);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_features_define
);


sub import_features_define {

    my ($file) = @_;
    my $result = Projects::Migration::IPGallery::Dao::FeatureOption::create_table(1);
    $result &= Projects::Migration::IPGallery::Dao::FeatureOptionSet::create_table(1);
    my $importer = Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile->new($feature_define_import_numofthreads);
    $importer->{stoponparseerrors} = !$dry;
    return $result && $importer->process($file,sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            my @featureoption_rows = ();
            my @featureoptionset_rows = ();
            foreach my $line (@$rows) {
                my $row = undef;
                if (not $importer->{parselines}) {
                    eval {
                        $row = Projects::Migration::IPGallery::FeaturesDefineParser::parse(\$line,$context->{grammar});
                    };
                    if ($@) {
                        if ($importer->{stoponparseerrors}) {
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
                                foreach my $setoptionitem (@{removeduplicates($option->{$setoption})}) {
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

            my $import_db = &get_import_db();
            if ((scalar @featureoption_rows) > 0) {
                $import_db->db_do_begin(
                    Projects::Migration::IPGallery::Dao::FeatureOption::getinsertstatement(),
                    Projects::Migration::IPGallery::Dao::FeatureOption::gettablename(),
                    #lock - $import_multithreading
                );
                $import_db->db_do_rowblock(\@featureoption_rows);
                $import_db->db_finish();
            }
            if ((scalar @featureoptionset_rows) > 0) {
                $import_db->db_do_begin(
                    Projects::Migration::IPGallery::Dao::FeatureOptionSet::getinsertstatement(),
                    Projects::Migration::IPGallery::Dao::FeatureOptionSet::gettablename(),
                    #lock
                );
                $import_db->db_do_rowblock(\@featureoptionset_rows);
                $import_db->db_finish();
            }
            return 1;
        }, sub {
            my ($context)= @_;
            if (not $importer->{parselines}) {
                eval {
                    $context->{grammar} = Projects::Migration::IPGallery::FeaturesDefineParser::create_grammar();
                };
                if ($@) {
                    fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
                }
            }
        },$import_multithreading);

}



1;
