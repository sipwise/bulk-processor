package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TrunkIdentification;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $PADDING
    $INCOMING
    $OUTGOING
);

my $field_name = "trunk identification number";
my $length = 10;
my @param_names = qw/direction trunk_group_number trunk_member_number/;

my @directions = ();
our $PADDING = '0';
push(@directions,$PADDING);
our $INCOMING = '1';
push(@directions,$INCOMING);
our $OUTGOING = '2';
push(@directions,$OUTGOING);

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($direction,$trunk_group_number,$trunk_member_number) = $self->_get_params(@_);
    die("invalid direction '$direction'") unless contains($direction,\@directions);
    die("invalid trunk group number '$trunk_group_number'") if length($trunk_group_number) != 4;
    die("invalid trunk member number '$trunk_member_number'") if length($trunk_member_number) != 4;
    return $direction . sprintf('%04d',$trunk_group_number) . sprintf('%04d',$trunk_member_number) . $TERMINATOR;

}

1;
