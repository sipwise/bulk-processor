package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SensorId;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "sensor id";
my $length = 8;
my @param_names = qw/rewritten sensor_id/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{rewritten} //= 0;
    #$self->{sensor_id} = substr($self->{sensor_id},1( if defined $self->{sensor_id};

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($rewritten,$sensor_id) = $self->_get_params(@_);
    die("invalid sensor id '$sensor_id'") unless length($sensor_id) == 6;
    return ($rewritten ? '1' : '0') . $sensor_id . $TERMINATOR;

}

1;
