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

use Test::More tests => 112;

require_ok('NGCP::BulkProcessor::Array');

NGCP::BulkProcessor::Array->import(qw(
    arrayeq
    seteq
    mapeq
    contains
    setcontains
    filter
    mergearrays
    reversearray
    removeduplicates
    getroundrobinitem
    itemcount
));

is(contains(), 0);
is(contains(undef, [], 0), 0);
is(contains('foo', [ qw(aa bb cc) ], 0), 0);
is(contains('foo', [ qw(aa bb cc) ], 1), 0);
is(contains('foo', [ qw(aa bb foo cc) ], 0), 1);
is(contains('foo', [ qw(aa bb FOO cc) ], 1), 1);

is(setcontains(), 1);
is(setcontains(undef, [], 0), 1);
is(setcontains([ qw(foo bar) ], [ qw(aa bb cc) ], 0), 0);
is(setcontains([ qw(foo bar) ], [ qw(aa bb cc) ], 1), 0);
is(setcontains([ qw(foo bar) ], [ qw(aa bb foo cc) ], 0), 0);
is(setcontains([ qw(foo bar) ], [ qw(aa bb foo bar cc) ], 0), 1);
is(setcontains([ qw(bar foo) ], [ qw(aa bb foo bar cc) ], 0), 1);
is(setcontains([ qw(aa bb cc) ], [ qw(aa bb cc) ], 0), 1);
is(setcontains([ qw(cc bb aa) ], [ qw(aa bb cc) ], 0), 1);
is(setcontains([ qw(foO bar) ], [ qw(aa bb fOo baR cc) ], 1), 1);
is(setcontains([ qw(bAr foo) ], [ qw(aa bb foO Bar cc) ], 1), 1);
is(setcontains([ qw(aA bb Cc) ], [ qw(Aa BB cC) ], 1), 1);
is(setcontains([ qw(Cc BB Aa) ], [ qw(aa bb CC) ], 1), 1);

is(arrayeq(), 1);
is(arrayeq(undef, undef), 1);
is(arrayeq([], []), 1);
is(arrayeq([ qw(aa bb) ], []), 0);
is(arrayeq([], [ qw(aa bb) ]), 0);
is(arrayeq([ qw(aa) ], [ qw(aa bb) ]), 0);
is(arrayeq([ qw(aa bb cc) ], [ qw(aa bbb cc) ]), 0);
is(arrayeq([ qw(bb cc aa) ], [ qw(aa bb cc) ]), 0);
is(arrayeq([ qw(aa bb cc) ], [ qw(aa bb cc) ]), 1);
is(arrayeq([ qw(aa bb cc) ], [ qw(aA bBb Cc) ], 1), 0);
is(arrayeq([ qw(Bb cc aa) ], [ qw(aa Bb cC) ], 1), 0);
is(arrayeq([ qw(aA Bb cc) ], [ qw(aa bb Cc) ], 1), 1);

is(seteq(), 1);
is(seteq(undef, undef), 1);
is(seteq([], []), 1);
is(seteq([ qw(aa bb) ], []), 0);
is(seteq([], [ qw(aa bb) ]), 0);
is(seteq([ qw(aa) ], [ qw(aa bb) ]), 0);
is(seteq([ qw(aa bb cc) ], [ qw(aa bbb cc) ]), 0);
is(seteq([ qw(bb cc aa) ], [ qw(aa bb cc) ]), 1);
is(seteq([ qw(aa bb cc) ], [ qw(aa bb cc) ]), 1);
is(seteq([ qw(aa bb cc) ], [ qw(aA bBb Cc) ], 1), 0);
is(seteq([ qw(Bb cc aa) ], [ qw(aa Bb cC) ], 1), 1);
is(seteq([ qw(aA Bb cc) ], [ qw(aa bb Cc) ], 1), 1);

is(mapeq(), 1);
is(mapeq(undef, undef), 1);
is(mapeq({}, {}), 1);
is(mapeq({ aa => 1, bb => 1 }, {}), 0);
is(mapeq({}, { aa => 1, bb => 1 }), 0);
is(mapeq({ aa => 1 }, { aa => 1, bb => 1 }), 0);
is(mapeq({ aa => 1, bb => 1, cc => 1 },
         { aa => 1, bbb => 1, cc => 1 }), 0);
is(mapeq({ aa => 1, bb => 1, cc => 1 },
         { aa => 2, bb => 2, cc => 2 }), 0);
is(mapeq({ aa => 1, bb => 1, cc => 1 },
         { aa => 1, bb => 1, cc => 1 }), 1);
is(mapeq({ aa => 1, bb => 1, cc => 1 },
         { aA => 1, bBb => 1, Cc => 1 }, 1), 0);
is(mapeq({ aA => 1, Bb => 1, cc => 1 },
         { aa => 1, bb => 1, Cc => 1 }, 1), 0);
is(mapeq({ aa => 'aa', bb => 'bb', cc => 'cc' },
         { aa => 'aA', bb => 'BB', cc => 'Cc' }, 1), 1);

is_deeply(filter(), []);
is_deeply(filter(undef, undef), []);
is_deeply(filter(undef, []), []);
is_deeply(filter([], undef), []);
is_deeply(filter([], []), []);
is_deeply(filter([], [ qw(aa bb cc) ]), []);
is_deeply(filter([ qw(aa bb cc) ], undef), [ qw(aa bb cc) ]);
is_deeply(filter([ qw(aa bb cc) ], []), []);
is_deeply(filter([ qw(zz yy xx) ], [ qw(aa bb cc) ]), []);
is_deeply(filter([ qw(aa) ], [ qw(aA bb cc) ]), [ ]);
is_deeply(filter([ qw(aa) ], [ qw(aa bb cc) ]), [ qw(aa) ]);
is_deeply(filter([ qw(aa bb cc) ], [ qw(aa bb cc) ]), [ qw(aa bb cc) ]);
is_deeply(filter([ qw(aa zz bb yy cc xx) ], [ qw(aa bb cc) ]), [ qw(aa bb cc) ]);
is_deeply(filter([ qw(aa bb cc) ], [ qw(aa zz bb yy cc xx) ]), [ qw(aa bb cc) ]);
is_deeply(filter([ qw(aA) ], [ qw(aa bb cc) ], 1), [ qw(aA) ]);
is_deeply(filter([ qw(aA Bb cc) ], [ qw(Aa bB CC) ], 1), [ qw(aA Bb cc) ]);
is_deeply(filter([ qw(aA Zz bB yy CC Xx) ], [ qw(AA Bb cc) ], 1), [ qw(aA bB CC) ]);
is_deeply(filter([ qw(aA Bb CC) ], [ qw(AA zZ bB Yy cc Xx) ], 1), [ qw(aA Bb CC) ]);

is_deeply(mergearrays(), []);
is_deeply(mergearrays([], []), []);
is_deeply(mergearrays([ qw(aa bb) ], []), [ qw(aa bb) ]);
is_deeply(mergearrays([ ], [ qw(aa bb) ]), [ qw(aa bb) ]);
is_deeply(mergearrays([ qw(aa bb) ], [ qw(cc dd) ]), [ qw(aa bb cc dd) ]);

is_deeply(reversearray(), []);
is_deeply(reversearray([]), []);
is_deeply(reversearray([ qw(aa bb cc) ], []), [ qw(cc bb aa) ]);

is_deeply(removeduplicates(), []);
is_deeply(removeduplicates([], 0), []);

is_deeply(removeduplicates([ qw(aa bb cc) ], 0), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(aa bb aa cc aa) ], 0), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(aa aa bb bb cc cc) ], 0), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(aa bb aa cc bb cc) ], 0), [ qw(aa bb cc) ]);

is_deeply(removeduplicates([ qw(Aa BB cC) ], 1), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(aA BB Aa Cc aa) ], 1), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(aA AA bB Bb CC cc) ], 1), [ qw(aa bb cc) ]);
is_deeply(removeduplicates([ qw(AA bB Aa cc Bb CC) ], 1), [ qw(aa bb cc) ]);

is_deeply([ getroundrobinitem() ], [ (undef, undef) ]);
is_deeply([ getroundrobinitem(undef, undef) ], [ (undef, undef) ]);
is_deeply([ getroundrobinitem([], 0) ], [ (undef, undef) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], -1) ], [ ('aa', 0) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 0) ], [ ('bb', 1) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 1) ], [ ('cc', 2) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 3) ], [ ('ee', 4) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 4) ], [ ('aa', 0) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 5) ], [ ('bb', 1) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 8) ], [ ('ee', 4) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 9) ], [ ('aa', 0) ]);
is_deeply([ getroundrobinitem([ qw(aa bb cc dd ee) ], 10) ], [ ('bb', 1) ]);

is(itemcount(), 0);
is(itemcount(undef, []), 0);
is(itemcount('foo', [ qw(aa bb cc aa zz aa aa) ]), 0);
is(itemcount('AA', [ qw(aa bb cc aa zz aa aa) ]), 0);
is(itemcount('aa', [ qw(aa bb cc aa zz aa aa) ]), 4);

is(itemcount('foo', [ qw(aa bb cc aa zz aa aa) ], 1), 0);
is(itemcount('AA', [ qw(aA bb cc Aa zz AA aa) ], 1), 4);
is(itemcount('aa', [ qw(aa bb cc aa zz aa aa) ], 1), 4);
