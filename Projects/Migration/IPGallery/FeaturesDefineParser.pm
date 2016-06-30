package Projects::Migration::IPGallery::FeaturesDefineParser;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

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

:start   ::= Record
:default ::= action => ::first

Record     ::= SubscriberNumber '{' Options '}'   action => _hash
Options ::= Option+                                  action => _list
Option ::= OptionName | OptionName '{' OptionValues '}' action => _test
OptionValues ::= OptionValue+                           action => _list

SubscriberNumber ~ [0-9]+
OptionName     ~ [-a-zA-Z_0-9]+
OptionValue     ~ [-a-zA-Z_0-9]+
whitespace ~ [\s]+
:discard   ~ whitespace
__GRAMMAR__

sub _hash {
    my ($closure,@blah) = @_;
    #print "blah";
    #print Dumper(\@_);
    #return { $_[1] => $_[3] };
}

sub _list {
    my ($closure,@blah) = @_;
    #print "blah1";
    #print Dumper(\@_);
    #return \@blah;
}

sub _test {
    my ($closure,@blah) = @_;
    #print "blah1";
    #print Dumper(\@_);
    #return \@blah;
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
    return $recce->value($closure);
    #my $value = $value_ref ? ${$value_ref} : 'No Parse';

}

1;
