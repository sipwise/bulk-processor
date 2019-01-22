package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ModuleCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw($TERMINATOR);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(
    $MODULE_CODE_611
    $MODULE_CODE_199
    $MODULE_CODE_104
);

my $field_name = "module code";
my $length = 4;
my @param_names = qw/module_code/;

my @module_codes = ();
our $MODULE_CODE_611 = '611';
push(@module_codes,$MODULE_CODE_611);
our $MODULE_CODE_199 = '199';
push(@module_codes,$MODULE_CODE_199);
our $MODULE_CODE_104 = '104';
push(@module_codes,$MODULE_CODE_104);

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

sub get_module_code {

    my $self = shift;
    return $self->{module_code};

}

sub get_hex {

    my $self = shift;
    my ($module_code) = $self->_get_params(@_);
    die("invalid module code '$module_code'") unless contains($module_code,\@module_codes);
    return $module_code . $TERMINATOR;

}

1;
