package NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::StructureCode;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Field qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::Projects::Export::Ama::Format::Field);
our @EXPORT_OK = qw(

);

my $field_name = "STRUCTURE CODE";
my $length = 4;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::Projects::Export::Ama::Format::Field->new(
        $class,
        name => $field_name,
        length => $length);
    my %params = @_;
    (
        $self->{has_modules},
        $self->{code},
    ) = @params{qw/
        has_modules
        code
    /};

    return $self;

}

sub get_hex {

    my $self = shift;
    return ($self->{has_modules} ? '4' : '0') . $self->{code};

}

1;
