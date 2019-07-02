package NGCP::BulkProcessor::Projects::Migration::IPGallery::FeaturesDefineParser;
use strict;

## no critic

use Marpa::R2;
use Data::Dumper::Concise;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    create_grammar
    parse
);

my $grammar = << '__GRAMMAR__';
lexeme default = latm => 1

:start           ::= Record
:default         ::= action => ::first

Record           ::= SubscriberNumber '{' Options '}'  action => _build_record
Options          ::= Option+                           action => _build_options
Option           ::= OptionName                        action => _build_option
                     | OptionName '{' OptionValues '}' action => _build_setoption
OptionValues     ::= OptionValue+                      action => _build_setoptionitems

SubscriberNumber ~ [0-9]+
OptionName       ~ [-a-zA-Z_0-9]+
OptionValue      ~ [-a-zA-Z_0-9 \t#*]+
whitespace       ~ [\s]+
:discard         ~ whitespace
__GRAMMAR__

my %setoptions_to_clean = (
    Incoming_Prefix_Barring => 1,
);

sub _build_record {

    my ($closure,$subscribernumber,$leftcb,$options,$rightcb) = @_;
    return { $subscribernumber => $options };

}

sub _build_options {

    my ($closure,@options) = @_;
    return \@options;

}

sub _build_option {

    my ($closure,$optionname) = @_;
    return $optionname;

}

sub _build_setoption {

    my ($closure,$optionname,$leftcb,$optionvalues,$rightcb) = @_;
    if (exists $setoptions_to_clean{$optionname}
        and $setoptions_to_clean{$optionname}) {
        my @cleanoptionvalues = map { local $_ = $_; s/(\t|#).*$//g; $_; } @$optionvalues;
        $optionvalues = \@cleanoptionvalues;
    }
    return { $optionname => $optionvalues };

}

sub _build_setoptionitems {

    my ($closure,@optionvalues) = @_;
    return \@optionvalues;

}

sub create_grammar {

    return Marpa::R2::Scanless::G->new({
            source => \$grammar,
        });

}

sub parse {

    my ($input_ref,$grammar) = @_;
    my $recce = Marpa::R2::Scanless::R->new({
        grammar => $grammar,
        semantics_package => __PACKAGE__,
    });
    $recce->read($input_ref);
    my $closure = {};
    my $value_ref = $recce->value($closure);
    return $value_ref ? ${$value_ref} : undef;

}

1;