package Projects::Migration::IPGallery::Import;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Projects::Migration::IPGallery::Settings qw(
    $defaultsettings
    update_settings
);
use Logging qw (
    getlogger
);
use LogError qw(
    fileprocessingwarn
    fileprocessingerror
);

#use FileProcessors::CSVFile;
use Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile;
use Projects::Migration::IPGallery::FeaturesDefineParser qw(
    create_grammar
    parse
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    import_features_define
);


sub import_features_define {

    my ($file) = @_;
    my $multithreading = 1;
    my $importer = Projects::Migration::IPGallery::FileProcessors::FeaturesDefineFile->new();
    return $importer->process($file,sub {
            my ($context,$rows,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $line (@$rows) {
                my $row = undef;
                if (not $importer->{parselines}) {
                    eval {
                        $row = parse(\$line,$context->{grammar});
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
                # continue to write to sqlite ...
            }
            return 1;
        }, sub {
            my ($context)= @_;
            if (not $importer->{parselines}) {
                eval {
                    $context->{grammar} = create_grammar();
                };
                if ($@) {
                    fileprocessingerror($context->{filename},$@,getlogger(__PACKAGE__));
                }
            }
        },$multithreading);

}



1;
