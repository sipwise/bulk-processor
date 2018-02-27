package NGCP::BulkProcessor::RestRequests::Trunk::Customers;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_ngcp_restapi

);

use NGCP::BulkProcessor::RestProcessor qw(
    copy_row
);

use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw();
use NGCP::BulkProcessor::RestItem qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::RestItem);
our @EXPORT_OK = qw(
    get_item
    create_item
    set_item
    update_item
    get_item_path

    $TERMINATED_STATE
);

my $get_restapi = \&get_ngcp_restapi;
my $resource = 'customers';
my $item_relation = 'ngcp:' . $resource;
my $get_item_path_query = sub {
    my ($id) = @_;
    return 'api/' . $resource . '/' . $id;
};
my $collection_path_query = 'api/' . $resource . '/';

my $fieldnames = [
    'add_vat',
    'billing_profile_definition',
    'billing_profile_id',
    'billing_profiles',
    'contact_id',
    'external_id',
    'invoice_email_template',
    'invoice_template',
    'max_subscribers',
    'passreset_email_template',
    'profile_package_id',
    'status',
    'subscriber_email_template',
    'type',
    'vat_rate',

    'id',
];

our $TERMINATED_STATE = 'terminated';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::RestItem->new($class,$fieldnames);

    copy_row($self,shift,$fieldnames);

    return $self;

}

sub get_item {

    my ($id,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->get(&$get_item_path_query($id),$headers),$load_recursive);

}

sub create_item {

    my ($data,$load,$load_recursive,$post_headers,$get_headers) = @_;
    my $restapi = &$get_restapi();
    if ($load) {
        return builditems_fromrows($restapi->post_get($collection_path_query,$data,$post_headers,$get_headers),$load_recursive);
    } else {
        my ($id) = $restapi->post($collection_path_query,$data,$post_headers);
        return $id;
    }

}

sub set_item {

    my ($id,$data,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->put(&$get_item_path_query($id),$data,$headers),$load_recursive);

}

sub update_item {

    my ($id,$data,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->patch(&$get_item_path_query($id),$data,$headers),$load_recursive);

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

sub get_item_path {

    my ($id) = @_;
    return &$get_item_path_query($id);

}

1;
