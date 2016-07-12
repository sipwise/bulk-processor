package NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_ngcp_restapi

);

use NGCP::BulkProcessor::RestProcessor qw(
    process_collection
    copy_row
);

use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw();
use NGCP::BulkProcessor::RestItem qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::RestItem);
our @EXPORT_OK = qw(
    process_items
);


my $get_item_path_query = sub {
    my ($billing_profile_id) = @_;
    return 'api/billingprofile/' . $billing_profile_id;
};
my $collection_path_query = 'api/billingprofiles/';
my $item_relation = 'ngcp:billingprofiles';
my $get_restapi = \&get_ngcp_restapi;

my $fieldnames = [
    'id',
    'start_date',
    'end_date',
    'billing_profile_id',
    'contract_id',
    'product_id',
    'network_id',
];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::RestItem->new($fieldnames);

    bless($self,$class);

    copy_row($self,shift,$fieldnames);

    return $self;

}



sub builditems_fromrows {

    my ($rows,$load_recursive) = @_;

    my @items = ();
    my $item;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $item = __PACKAGE__->new($row);

            # transformations go here ...

            push @items,$item;
        }
    }

    return \@items;

}

sub process_items {

    my %params = @_;
    my ($process_code,
        $blocksize,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            blocksize
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            load_recursive
        /};

    return process_collection(
        get_restapi                     => $get_restapi,
        path_query                      => $collection_path_query,
        headers                         => undef, #faketime,..
        extract_collection_items_params => { $NGCP::BulkProcessor::RestConnectors::NGCPRestApi::ITEM_REL_PARAM => $item_relation },
        process_code                    => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,builditems_fromrows($rowblock,$load_recursive),$row_offset);
            },
        blocksize                       => $blocksize,
        init_process_context_code       => $init_process_context_code,
        uninit_process_context_code     => $uninit_process_context_code,
        multithreading                  => $multithreading,
        collectionprocessing_threads    => $numofthreads,
    );
}

1;
