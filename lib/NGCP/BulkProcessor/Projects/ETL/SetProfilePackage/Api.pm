package NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Api;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
#use DateTime qw();

#use NGCP::BulkProcessor::Globals qw(
#    $system_abbreviation
#);

use NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Settings qw(
    $dry
    $skip_errors

    $set_profile_package_multithreading
    $set_profile_package_numofthreads

);

use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    set_profile_package
);

sub set_profile_package {

    my $result = 1;

    return $result && NGCP::BulkProcessor::RestRequests::Trunk::Customers::process_items(
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
        multithreading => $set_profile_package_multithreading,
        numofthreads => $set_profile_package_numofthreads,
    );
}

1;
