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

use Test::More tests => 25;

require_ok('NGCP::BulkProcessor::Array');

NGCP::BulkProcessor::Array->import(qw(
    contains
    mergearrays
    reversearray
    removeduplicates
));

# Container functions
is(contains(), 0);
is(contains(undef, [], 0), 0);
is(contains('foo', [ qw(aa bb cc) ], 0), 0);
is(contains('foo', [ qw(aa bb cc) ], 1), 0);
is(contains('foo', [ qw(aa bb foo cc) ], 0), 1);
is(contains('foo', [ qw(aa bb FOO cc) ], 1), 1);

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

is_deeply(removeduplicates([ qw(Aa BB cC) ], 1), [ qw(Aa BB cC) ]);
is_deeply(removeduplicates([ qw(aA BB Aa Cc aa) ], 1), [ qw(aA BB Cc) ]);
is_deeply(removeduplicates([ qw(aA AA bB Bb CC cc) ], 1), [ qw(aA bB CC) ]);
is_deeply(removeduplicates([ qw(AA bB Aa cc Bb CC) ], 1), [ qw(AA bB cc) ]);
