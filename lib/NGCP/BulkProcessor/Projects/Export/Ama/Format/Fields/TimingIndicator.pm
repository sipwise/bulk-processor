package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::TimingIndicator;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);
use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $TIMING_GUARD_DEFAULT
    $TIMING_GUARD_ELAPSED_TIME_ESTIMATED
    $CALLED_PARTY_DEFAULT
    $CALLED_PARTY_OFF_HOOK
    $LONG_DURATION_DEFAULT
    $LONG_DURATION_START
    $LONG_DURATION_CONTINUE
    $LONG_DURATION_END
);

my $field_name = "timing indicator";
my $length = 6;
my @param_names = qw/timing_guard called_party long_duration/;

our $TIMING_GUARD_DEFAULT = '0';
our $TIMING_GUARD_ELAPSED_TIME_ESTIMATED = '2';
our $CALLED_PARTY_DEFAULT = '0';
our $CALLED_PARTY_OFF_HOOK = '1';
my @long_duration_modes = ();
our $LONG_DURATION_DEFAULT = '0';
push(@long_duration_modes,$LONG_DURATION_DEFAULT);
our $LONG_DURATION_START = '1';
push(@long_duration_modes,$LONG_DURATION_START);
our $LONG_DURATION_CONTINUE = '2';
push(@long_duration_modes,$LONG_DURATION_CONTINUE);
our $LONG_DURATION_END = '3';
push(@long_duration_modes,$LONG_DURATION_END);

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{timing_guard} //= $TIMING_GUARD_DEFAULT;
    $self->{called_party} //= $CALLED_PARTY_DEFAULT;
    $self->{long_duration} //= $LONG_DURATION_DEFAULT;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($timing_guard,$called_party,$long_duration) = $self->_get_params(@_);
    die("invalid timing guard '$timing_guard'") unless contains($timing_guard,[$TIMING_GUARD_DEFAULT, $TIMING_GUARD_ELAPSED_TIME_ESTIMATED]);
    die("invalid called party '$called_party'") unless contains($called_party,[$CALLED_PARTY_DEFAULT, $CALLED_PARTY_OFF_HOOK]);
    die("invalid long duration '$long_duration'") unless contains($long_duration,\@long_duration_modes);
    return $timing_guard . $called_party . $long_duration . '00' . $TERMINATOR;

}

1;
