package NGCP::BulkProcessor::Projects::Migration::IPGallery::SubscriberProvisioning;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber qw();

use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    destroy_all_dbs
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    test
);

sub test {

    my $result = 1;

    return $result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;

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
