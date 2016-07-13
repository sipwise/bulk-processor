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
    get_item
    create_item
    process_items
);

my $get_restapi = \&get_ngcp_restapi;
my $resource = 'billingprofiles';
my $item_relation = 'ngcp:' . $resource;
my $get_item_path_query = sub {
    my ($contact_id) = @_;
    return 'api/' . $resource . '/' . $contact_id;
};
my $collection_path_query = 'api/' . $resource . '/';

my $fieldnames = [
    'currency',
    'fraud_daily_limit',
    'fraud_daily_lock',
    'fraud_daily_notify',
    'fraud_interval_limit',
    'fraud_interval_lock',
    'fraud_interval_notify',
    'fraud_use_reseller_rates',
    'handle',
    'interval_charge',
    'interval_free_cash',
    'interval_free_time',
    'name',
    'peaktime_special',
    'peaktime_weekdays',
    'prepaid',
    'reseller_id',
];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::RestItem->new($fieldnames);

    bless($self,$class);

    copy_row($self,shift,$fieldnames);

    return $self;

}

sub get_item {

    my ($contact_id,$load_recursive) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->get(&$get_item_path_query($contact_id)),$load_recursive);

}

sub create_item {

    my ($data,$load,$load_recursive,$post_headers,$get_headers) = @_;
    my $restapi = &$get_restapi();
    if ($load) {
        return builditems_fromrows($restapi->post_get($collection_path_query,$data,$post_headers,$get_headers),$load_recursive);
    } else {
        my ($contact_id) = $restapi->post($collection_path_query,$data,$post_headers);
        return $contact_id;
    }

}

sub builditems_fromrows {

    my ($rows,$load_recursive) = @_;

    my $item;

    if (defined $rows and ref $rows eq 'ARRAY') {
        my @items = ();
        foreach my $row (@$rows) {
            $item = __PACKAGE__->new($row);

            # transformations go here ...

            push @items,$item;
        }
        return \@items;
    } elsif (defined $rows and ref $rows eq 'HASH') {
        $item = __PACKAGE__->new($rows);
        return $item;
    }
    return undef;

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
