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
use NGCP::BulkProcessor::RestItem qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::RestItem);
our @EXPORT_OK = qw(
    gettablename

    insert_row
);


my $tablename = 'billing_mappings';
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

sub insert_row {

    my ($data,$insert_ignore) = @_;
    check_table();
    #return insert_record($get_db,$tablename,$data,$insert_ignore,$unique_fields) = @_;

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

sub gettablename {

    return $tablename;

}

1;
