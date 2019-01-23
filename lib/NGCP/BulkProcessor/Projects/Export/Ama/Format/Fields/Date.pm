package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);
#use NGCP::BulkProcessor::Calendar qw(datetime_from_string);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    get_ama_date
);

my $field_name = "date";
my $length = 6;
my @param_names = qw/date dt/;

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
    my ($date,$dt) = $self->_get_params(@_);
    die("invalid date '$date'") unless length($date) == 5;
    return $date . $TERMINATOR;

}

sub get_ama_date {

    my $dt = shift; #datetime_from_string(shift);
    return substr($dt->year(),-1) . sprintf('%02d',$dt->month()) . sprintf('%02d',$dt->day());

}

1;
