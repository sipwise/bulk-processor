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

use Test::More;

unless (eval { require NGCP::BulkProcessor::SqlConnectors::MySQLDB; 1; }) {
    plan skip_all => 'cannot load NGCP::BulkProcessor::SqlConnectors::MySQLDB (DBD::mysql missing?)';
}

my $dsn = NGCP::BulkProcessor::SqlConnectors::MySQLDB::_dsn('accounting','db.example.org',3307);

like($dsn, qr/^dbi:mysql:database=accounting;host=db\.example\.org;port=3307;/,
    'dsn starts with database, host and port');

my %options = map { split(/=/, $_, 2); } split(/;/, substr($dsn, length('dbi:mysql:')));

# A silently vanished server (host down, VIP failover, dropped firewall
# state) must not block a connect or a query forever:
cmp_ok($options{mysql_connect_timeout} // 0, '>', 0, 'mysql_connect_timeout set');
cmp_ok($options{mysql_write_timeout} // 0, '>', 0, 'mysql_write_timeout set');
cmp_ok($options{mysql_read_timeout} // 0, '>', 0, 'mysql_read_timeout set');
# but leave enough time for long running queries:
cmp_ok($options{mysql_read_timeout} // 0, '>=', 600, 'mysql_read_timeout allows long running queries');

done_testing();
