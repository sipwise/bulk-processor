package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecorderGenericIssue;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "generic id";
my $length = 6;
my @param_names = qw/generic_issue_number point_issue_level overwrite_level/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{generic_issue_number} //= 0;
    $self->{point_issue_level} //= 0;
    $self->{overwrite_level} //= 0;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($generic_issue_number,$point_issue_level,$overwrite_level) = $self->_get_params(@_);
    die("invalid generic issue number '$generic_issue_number'") if length($generic_issue_number) < 1 or length($generic_issue_number) > 2;
    die("invalid point issue level '$point_issue_level'") if length($point_issue_level) < 1 or length($point_issue_level) > 2;
    die("invalid overwrite level '$overwrite_level'") if length($overwrite_level) != 1;
    return sprintf('%02d',$generic_issue_number) . sprintf('%02d',$point_issue_level) . $overwrite_level . $TERMINATOR;

}

1;
