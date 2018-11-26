package NGCP::BulkProcessor::RestRequests::Trunk::ProfilePackages;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_ngcp_restapi

);

use NGCP::BulkProcessor::RestProcessor qw(
    copy_row
    get_query_string
);

use NGCP::BulkProcessor::RestConnectors::NGCPRestApi qw();
use NGCP::BulkProcessor::RestItem qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::RestItem);
our @EXPORT_OK = qw(
    get_item
    create_item
    get_item_path
    findby_resellerid
    findby_name
);

my $get_restapi = \&get_ngcp_restapi;
my $resource = 'profilepackages';
my $item_relation = 'ngcp:' . $resource;
my $get_item_path_query = sub {
    my ($id) = @_;
    return 'api/' . $resource . '/' . $id;
};
my $collection_path_query = 'api/' . $resource . '/';

my $findby_resellerid_path_query = sub {
    my ($reseller_id) = @_;
    my $filters = {};
    $filters->{reseller_id} = $reseller_id if defined $reseller_id;
    return 'api/' . $resource . '/' . get_query_string($filters);
};
my $findby_name_path_query = sub {
    my ($name) = @_;
    my $filters = {};
    $filters->{name} = $name if defined $name;
    return 'api/' . $resource . '/' . get_query_string($filters);
};

my $fieldnames = [

    'balance_interval_start_mode',
    'balance_interval_unit',
    'balance_interval_value',
    'carry_over_mode',
    'description',
    'initial_balance',
    'initial_profiles',
    'name',
    'notopup_discard_intervals',
    'reseller_id',
    'service_charge',
    'timely_duration_unit',
    'timely_duration_value',
    'topup_lock_level',
    'topup_profiles',
    'underrun_lock_level',
    'underrun_lock_threshold',
    'underrun_profile_threshold',
    'underrun_profiles',

    'id',
];

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

sub findby_resellerid {

    my ($reseller_id,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->extract_collection_items($restapi->get(&$findby_resellerid_path_query($reseller_id),$headers),undef,undef,
        { $NGCP::BulkProcessor::RestConnectors::NGCPRestApi::ITEM_REL_PARAM => $item_relation }),$load_recursive);

}

sub findby_name {

    my ($name,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->extract_collection_items($restapi->get(&$findby_name_path_query($name),$headers),undef,undef,
        { $NGCP::BulkProcessor::RestConnectors::NGCPRestApi::ITEM_REL_PARAM => $item_relation }),$load_recursive);

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
