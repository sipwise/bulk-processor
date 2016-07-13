package NGCP::BulkProcessor::Projects::t::ApiTest;
use strict;

## no critic

use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    test
);

sub test {

    my $result = 1;

    return $result && NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles::process_items(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            print "!!!!!!$row_offset!!!!!!!\n";
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            destroy_all_dbs();
        },
        load_recursive => 1,
        multithreading => 1,
        numofthreads => 4,
    );
}

1;
