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

use Test::More tests => 4;

require_ok('NGCP::BulkProcessor::Utils');

NGCP::BulkProcessor::Utils->import(qw(
    zerofill
));

# zerofill()
is(zerofill(0, 4), '0000');
is(zerofill(25, 4), '0025');
is(zerofill(1000, 4), '1000');
