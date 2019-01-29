package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);
#use NGCP::BulkProcessor::Calendar qw(datetime_from_string);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_connect_time
);

my $field_name = "connect time";
my $length = 8;
my @param_names = qw/connect_time/;

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
    my ($connect_time) = $self->_get_params(@_);
    die("invalid connect time '$connect_time'") unless length($connect_time) == 7;
    return $connect_time . $TERMINATOR;

}

sub get_connect_time {

    my $dt = shift; #datetime_from_string(shift);
    return sprintf('%02d',$dt->hour()) . sprintf('%02d',$dt->minute()) . sprintf('%02d',$dt->second()) .
        substr(sprintf("%03d",$dt->millisecond // 0),0,1);

}

1;
