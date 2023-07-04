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

use Test::More tests => 72;
use Time::Local;

require_ok('NGCP::BulkProcessor::Utils');

NGCP::BulkProcessor::Utils->import(qw(
    zerofill
    humanize_bytes
    kbytes2gigs
    secs_to_years
    timestampdigits
    datestampdigits
    timestamp
    timestamp_fromepochsecs
    datestamp
    get_year
    get_year_month
    get_year_month_day
    add_months
));

# zerofill()
is(zerofill(0, 4), '0000');
is(zerofill(25, 4), '0025');
is(zerofill(1000, 4), '1000');

# Unit conversion
is(humanize_bytes(1), '1 bytes');
is(humanize_bytes(1024), '1 KiB');
is(humanize_bytes(1024 ** 2), '1 MiB');
is(humanize_bytes(1024 ** 3), '1 GiB');
is(humanize_bytes(1024 ** 4), '1 TiB');
is(humanize_bytes(1024 ** 5), '1 PiB');

is(humanize_bytes(2), '2 bytes');
is(humanize_bytes(2 * 1024), '2 KiB');
is(humanize_bytes(2 * (1024 ** 2) + 1), '2 MiB');
is(humanize_bytes(2 * (1024 ** 3) + 1024 + 1), '2 GiB');
is(humanize_bytes(2 * (1024 ** 4) + (1024 ** 2) + 1024 + 1), '2 TiB');

is(humanize_bytes(920), '920 bytes');
is(humanize_bytes(920 * 1024), '920 KiB');
is(humanize_bytes(942080 * 1024), '920 MiB');
is(humanize_bytes(964689920 * 1024), '920 GiB');

is(humanize_bytes(920, 1000), '920 bytes');
is(humanize_bytes(920000, 1000), '920 kB');
is(humanize_bytes(920000000, 1000), '920 MB');
is(humanize_bytes(920000000000, 1000), '920 GB');

is(humanize_bytes(942172), '920.08 KiB');
is(humanize_bytes(965632092), '920.89 MiB');

is(humanize_bytes(920920, 1000), '920.92 kB');
is(humanize_bytes(920920920, 1000), '920.92 MB');

is(humanize_bytes(942172, 1024, 1), '920 KiB');
is(humanize_bytes(965632092, 1024, 1), '920 MiB');

is(humanize_bytes(920920, 1000, 1), '920 kB');
is(humanize_bytes(920920920, 1000,1 ), '920 MB');

is(kbytes2gigs(1), '1 KiB');
is(kbytes2gigs(1024), '1 MiB');
is(kbytes2gigs(1024 ** 2), '1 GiB');

is(kbytes2gigs(2), '2 KiB');
is(kbytes2gigs(2 * 1024 + 1), '2 MiB');
is(kbytes2gigs(2 * 1024 ** 2 + 1024 + 1), '2 GiB');

is(kbytes2gigs(920), '920 KiB');
is(kbytes2gigs(920 * 1024), '920 MiB');
is(kbytes2gigs(920 * 1024 ** 2), '920 GiB');

is(kbytes2gigs(920, 1000), '920 kB');
is(kbytes2gigs(920 * 1000, 1000), '920 MB');
is(kbytes2gigs(920 * 1000 ** 2, 1000), '920 GB');

is(kbytes2gigs(92 + 920 * 1024), '920.08 MiB');
is(kbytes2gigs(92 + 920 * 1024 + 920 * 1024 ** 2), '920.89 GiB');

is(kbytes2gigs(920920, 1000), '920.92 MB');
is(kbytes2gigs(920920920, 1000), '920.92 GB');

is(kbytes2gigs(92 + 920 * 1024, 1024, 1), '920 MiB');
is(kbytes2gigs(92 + 920 * 1024 + 920 * 1024 ** 2, 1024, 1), '920 GiB');

is(kbytes2gigs(920920, 1000, 1), '920 MB');
is(kbytes2gigs(920920920, 1000,1 ), '920 GB');

# secs_to_years()
is(secs_to_years(1), '1 second');
is(secs_to_years(59), '59 seconds');
is(secs_to_years(3661), '1 hour, 1 minute, 1 second');
is(secs_to_years(7322), '2 hours, 2 minutes, 2 seconds');
is(secs_to_years(86461), '1 day, 0 hours, 1 minute, 1 second');
is(secs_to_years(691261), '8 days, 0 hours, 1 minute, 1 second');

# time functions
my $time = timelocal(58, 59, 23, 2, 10, 2042);

is(timestampdigits($time), '20421102235958');
is(datestampdigits($time), '20421102');
is(timestamp($time), '2042-11-02 23:59:58');
is(timestamp_fromepochsecs($time), '2042-11-02 23:59:58');
is(datestamp($time), '2042-11-02');
is(get_year($time), '2042');
is_deeply([ get_year_month($time) ], [ '2042', '11' ]);
is_deeply([ get_year_month_day($time) ], [ '2042', '11', '02' ]);

is_deeply([ add_months(1, 2042, 11) ] , [ 12, 2042 ]);
is_deeply([ add_months(1, 2042, 12) ] , [ 1, 2043 ]);
is_deeply([ add_months(12, 2042, 0) ] , [ 12, 2042 ]);
is_deeply([ add_months(12, 2042, 1) ] , [ 1, 2043 ]);
is_deeply([ add_months(1, 2042, 25) ] , [ 2, 2044 ]);
is_deeply([ add_months(0, 2042, 2) ] , [ undef, undef ]);
is_deeply([ add_months(13, 2042, 2) ] , [ undef, undef ]);
