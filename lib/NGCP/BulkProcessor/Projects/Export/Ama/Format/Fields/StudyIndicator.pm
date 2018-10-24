package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StudyIndicator;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

#use NGCP::BulkProcessor::Utils qw(zerofill);
use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $STUDY_TYPE_A_NA
    $STUDY_TYPE_A_SLUS
    $STUDY_TYPE_B_NA
    $STUDY_TYPE_B_OBSERVED
    $STUDY_TYPE_B_UNANSWERED_CALL_RECORDING
    $STUDY_TYPE_B_OBSERVED_UNANSWERED_CALL_RECORDING
    $STUDY_TYPE_C_NA
    $STUDY_TYPE_C_GENERATED
    $TEST_CALL_NA
    $TEST_CALL
    $NUMBER_NANP_NA
    $NUMBER_NANP_CALLER
    $NUMBER_NANP_CALLEE
    $NUMBER_NANP_CALLER_CALLEE
);

my $field_name = "study indicator";
my $length = 8;
my @param_names = qw/study_type_a study_type_b study_type_c test_call_ind number_nanp_ind/;

my @study_a_modes = ();
our $STUDY_TYPE_A_NA = '0';
push(@study_a_modes,$STUDY_TYPE_A_NA);
our $STUDY_TYPE_A_SLUS = '2';
push(@study_a_modes,$STUDY_TYPE_A_SLUS);
my @study_b_modes = ();
our $STUDY_TYPE_B_NA = '0';
push(@study_b_modes,$STUDY_TYPE_B_NA);
our $STUDY_TYPE_B_OBSERVED = '1';
push(@study_b_modes,$STUDY_TYPE_B_OBSERVED);
our $STUDY_TYPE_B_UNANSWERED_CALL_RECORDING = '2';
push(@study_b_modes,$STUDY_TYPE_B_UNANSWERED_CALL_RECORDING);
our $STUDY_TYPE_B_OBSERVED_UNANSWERED_CALL_RECORDING = '3';
push(@study_b_modes,$STUDY_TYPE_B_OBSERVED_UNANSWERED_CALL_RECORDING);
my @study_c_modes = ();
our $STUDY_TYPE_C_NA = '0';
push(@study_c_modes,$STUDY_TYPE_C_NA);
our $STUDY_TYPE_C_GENERATED = '2';
push(@study_c_modes,$STUDY_TYPE_C_GENERATED);
our $TEST_CALL_NA = '0';
our $TEST_CALL = '1';
my @number_nanp_modes = ();
our $NUMBER_NANP_NA = '0';
push(@number_nanp_modes,$NUMBER_NANP_NA);
our $NUMBER_NANP_CALLER = '1';
push(@number_nanp_modes,$NUMBER_NANP_CALLER);
our $NUMBER_NANP_CALLEE = '2';
push(@number_nanp_modes,$NUMBER_NANP_CALLEE);
our $NUMBER_NANP_CALLER_CALLEE = '3';
push(@number_nanp_modes,$NUMBER_NANP_CALLER_CALLEE);

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length,
        @_);
    $self->{study_type_a} //= $STUDY_TYPE_A_NA;
    $self->{study_type_b} //= $STUDY_TYPE_B_NA;
    $self->{study_type_c} //= $STUDY_TYPE_C_NA;
    $self->{test_call_ind} //= $TEST_CALL_NA;
    $self->{number_nanp_ind} //= $NUMBER_NANP_NA;

    return $self;

}

sub _get_param_names {

    my $self = shift;
    return @param_names;

}

sub get_hex {

    my $self = shift;
    my ($study_type_a,$study_type_b,$study_type_c,$test_call_ind,$number_nanp_ind) = $self->_get_params(@_);
    die("invalid study type a '$study_type_a'") unless contains($study_type_a,\@study_a_modes);
    die("invalid study type b '$study_type_b'") unless contains($study_type_b,\@study_b_modes);
    die("invalid study type c '$study_type_c'") unless contains($study_type_c,\@study_c_modes);
    die("invalid test call ind '$test_call_ind'") unless contains($test_call_ind,[$TEST_CALL_NA, $TEST_CALL]);
    die("invalid number nanp ind '$number_nanp_ind'") unless contains($number_nanp_ind,\@number_nanp_modes);
    return $study_type_a . $study_type_b . $study_type_c . $test_call_ind . '0' . $number_nanp_ind . '0' . $TERMINATOR;

}

1;
