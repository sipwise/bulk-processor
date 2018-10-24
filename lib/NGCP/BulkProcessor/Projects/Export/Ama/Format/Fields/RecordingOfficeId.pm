package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::RecordingOfficeId;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "recording office id";
my $length = 8;
my @param_names = qw/padding recording_office_id/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{padding} //= 0;
    #$self->{sensor_id} = substr($self->{sensor_id},1( if defined $self->{sensor_id};

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($padding,$recording_office_id) = $self->_get_params(@_);
    die("invalid recording office id '$recording_office_id'") unless length($recording_office_id) == 6;
    return ($padding ? '1' : '0') . $recording_office_id . $TERMINATOR;

}

1;
