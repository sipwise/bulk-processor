#!/usr/bin/perl
use warnings;
use strict;

use Marpa::R2;
use Data::Dumper;


my $input = do { local $/; <DATA> };

my $dsl = << '__GRAMMAR__';

lexeme default = latm => 1

:start   ::= List
:default ::= action => ::first

List      ::= Hash+                      action => list
Hash      ::= String '{' Pairs '}'       action => hash
Pairs     ::= Pair+                      action => list
Pair      ::= String Value ';'           action => pair
            | Hash
Value     ::= Simple
            | Bracketed
Bracketed ::= '[' String ']'             action => second
Simple    ::= String

String     ~ [-a-zA-Z_0-9]+
whitespace ~ [\s] +
:discard   ~ whitespace

__GRAMMAR__

sub hash { +{ $_[1] => $_[3] } }

sub pair { +{ $_[1] => $_[2] } }

sub second { [ @_[ 2 .. $#_-1 ] ] }

sub list { shift; \@_ }

my $grammar = Marpa::R2::Scanless::G->new( { source => \$dsl } );
my $recce = Marpa::R2::Scanless::R->new(
    { grammar => $grammar, semantics_package => 'main' } );
#my $input = '42 * 1 + 7';
$recce->read( \$input );

my $value_ref = $recce->value;
my $value = $value_ref ? ${$value_ref} : 'No Parse';

print Dumper $value;

#my $parser = 'Marpa::R2::Scanless::G'->new({ source => \$grammar });

#print Dumper $parser->parse(\$input, 'main', { trace_terminals => 1 });

__DATA__
bob {
    ed {
        larry {
            rule5 {
                option {
                    disable-server-response-inspection no;
                }
                tag [ some_tag ];
                from [ prod-L3 ];
                to [ corp-L3 ];
                source [ any ];
                destination [ any ];
                source-user [ any ];
                category [ any ];
                application [ any ];
                service [ any ];
                hip-profiles [ any ];
                log-start no;
                log-end yes;
                negate-source no;
                negate-destination no;
                action allow;
                log-setting orion_log;
            }
            rule6 {
                option {
                    disable-server-response-inspection no;
                }
                tag [ some_tag ];
                from [ prod-L3 ];
                to [ corp-L3 ];
                source [ any ];
                destination [ any ];
                source-user [ any ];
                category [ any ];
                application [ any ];
                service [ any ];
                hip-profiles [ any ];
                log-start no;
                log-end yes;
                negate-source no;
                negate-destination no;
                action allow;
                log-setting orion_log;
            }
        }
    }
}
