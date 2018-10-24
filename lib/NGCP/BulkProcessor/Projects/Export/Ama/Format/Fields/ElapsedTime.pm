package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Utils qw(to_duration_string);
#use NGCP::BulkProcessor::Calendar qw(datetime_from_string);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_elapsed_time
);

my $field_name = "elapsed time";
my $length = 10;
my @param_names = qw/elapsed_time/;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    #$self->{padding} //= 0;
    #$self->{sensor_id} = substr($self->{sensor_id},1( if defined $self->{sensor_id};

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($elapsed_time) = $self->_get_params(@_);
    die("invalid elapsed time '$elapsed_time'") unless length($elapsed_time) == 9;
    return $elapsed_time . $TERMINATOR;

}

sub get_elapsed_time {

    my $duration_secs = shift;
    my ($pretty,$years,$months,$days,$hours,$minutes,$seconds) = to_duration_string($duration_secs,'minutes','seconds',3,undef);
    return '0' . sprintf('%05d',$minutes) . sprintf('%02d',$seconds) . substr(sprintf("%03d",int(($seconds - int($seconds)) * 1000.0)),0,1);

}

1;
