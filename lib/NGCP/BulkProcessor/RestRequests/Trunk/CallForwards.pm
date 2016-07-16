package NGCP::BulkProcessor::RestRequests::Trunk::CallForwards;
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
    get_item_path
    set_item
    update_item
    delete_item
);

my $get_restapi = \&get_ngcp_restapi;
my $resource = 'callforwards';
my $item_relation = 'ngcp:' . $resource;
my $get_item_path_query = sub {
    my ($id) = @_;
    return 'api/' . $resource . '/' . $id;
};
my $collection_path_query = 'api/' . $resource . '/';

my $fieldnames = [
    'cfu',
    'cfb',
    'cft',
    'cfna',
];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::RestItem->new($fieldnames);

    bless($self,$class);

    copy_row($self,shift,$fieldnames);

    return $self;

}

sub get_item {

    my ($id,$load_recursive,$headers) = @_;
    my $restapi = &$get_restapi();
    return builditems_fromrows($restapi->get(&$get_item_path_query($id),$headers),$load_recursive);

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

sub delete_item {

    my ($id,$headers) = @_;
    my $restapi = &$get_restapi();
    ($id) = $restapi->delete(&$get_item_path_query($id),$headers);
    return $id;

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
