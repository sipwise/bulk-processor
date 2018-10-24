use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510 qw();

my $test = NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510->new(
    x => "y",
);
