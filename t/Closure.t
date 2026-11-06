#!/usr/bin/perl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

use strict;
use warnings;
use utf8;

use Test::More;
use JSON::PP qw();

eval { require NGCP::BulkProcessor::Closure; 1; }
    or plan skip_all => "NGCP::BulkProcessor::Closure not loadable: $@";

sub js {
    my ($code,$context,@args) = @_;
    return NGCP::BulkProcessor::Closure->new($code,$context,'test')->calc($context,@args);
}

# values into js
is(js('function(a) { return a; }',undef,"it's a \\ test"), "it's a \\ test", 'quotes and backslashes');
is(js('function(a) { return a; }',undef,'Grüße 😀'), 'Grüße 😀', 'non-ascii');
is(js('function() { return x + 1; }',{ x => 5 }), '51', 'plain values arrive as strings');
is(js('function() { return h.a[1].b; }',{ h => { a => [ 1, { b => 'c' } ] } }), 'c', 'nested hash/array context');
is(js('function(a) { return a.x; }',undef,{ x => 'y' }), 'y', 'hash argument');
is(js('function() { return v; }',{ v => 'first' }), 'first', 'context var first call');
my $c = NGCP::BulkProcessor::Closure->new('function() { return w; }',{ w => 1 },'test');
is($c->calc({ w => 2 }), 2, 'context var updated on next calc');

# values back to perl
is(js('function() { return 1.5; }'), 1.5, 'decimal');
is(js('function() { return true; }'), 1, 'true is 1');
is(js('function() { return false; }'), 0, 'false is 0');
is_deeply(js('function() { return { a: [ true, null, false ], b: null, c: { d: "e" } }; }'),
    { a => [ 1, undef, 0 ], b => undef, c => { d => 'e' } }, 'nested structure with bools and nulls');
is(js('function() { return undefined; }'), undef, 'undefined is undef');
is(js('function() { return new Date(Date.UTC(2026,0,1)); }'), 1767225600, 'date is epoch seconds');
eval { js('function() { return function() {}; }'); };
like($@, qr/^test: .*unsupported js value/, 'returning a function dies');

# helpers
is(js('function() { return sprintf("%05.1f|%s", 3.14159, "x"); }'), '003.1|x', 'sprintf');
is(js('function() { return quotemeta("a.b"); }'), 'a\\.b', 'quotemeta');
is(js('function() { return to_json({ a: 1 }); }'), '{"a":1}', 'to_json');
is(js('function() { stash_set("k", { x: [ 1, 2 ] }); return stash_get("k").x[1]; }'), 2, 'stash round trip');
is(js('function() { return f("a", true); }',{ f => sub { return join('|',@_); } }), 'a|1', 'perl helper args unboxed');
is(js('function() { return f().x; }',{ f => sub { return bless { x => 'y' }, 'Foo'; } }), 'y', 'blessed helper result is unblessed');
is(js('function() { return to_json(1); }',{ to_json => sub { return 'overridden'; } }), 'overridden', 'context overrides default helper');
eval { js('function() { return f(); }',{ f => sub { die("perl failed\n"); } }); };
like($@, qr/^test: .*perl failed/, 'perl helper error reaches caller');
is(js('function() { return [ f(1), f(0) ]; }',{ f => sub { return $_[0] ? JSON::PP::true : JSON::PP::false; } })->[1], 0, 'perl helper bool result is 0/1');
my %env = map { $_ => 1; } @{js('function() { return get_env(); }',{ myvar => 1 })};
ok($env{myvar} && !$env{_func}, 'get_env lists context vars, not _func');
ok($env{get_env} && $env{sprintf} && $env{stash_get} && !$env{Math} && !$env{_builtins}, 'get_env lists helpers, not js builtins');

# errors
eval { js('function() { return 1 +; }'); };
like($@, qr/^test: /, 'syntax error names the closure');
$c = NGCP::BulkProcessor::Closure->new('function(a) { if (a == 1) throw new Error("boom"); return "ok"; }',undef,'test');
eval { $c->calc(undef,1); };
like($@, qr/^test: .*boom/, 'throw names the closure');
is($c->calc(undef,0), 'ok', 'engine usable after error');

# perl closures
sub pl {
    my ($code,@args) = @_;
    return NGCP::BulkProcessor::Closure->new($code,undef,'test')->calc(undef,@args);
}
is(pl('sub { my $x = eval { die("caught\n"); }; return "ok"; }'), 'ok', 'perl closure with caught error does not die');
eval { pl('sub { die("perl closure failed\n"); }'); };
like($@, qr/^test: perl closure failed/, 'perl closure error names the closure');

# cleanup
NGCP::BulkProcessor::Closure::cleanup();
NGCP::BulkProcessor::Closure::cleanup();
is($c->calc(undef,0), 'ok', 'closure usable after cleanup');
NGCP::BulkProcessor::Closure::cleanup();

done_testing();
