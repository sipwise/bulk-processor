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

unless (eval { require NGCP::BulkProcessor::SqlConnectors::PostgreSQLDB; 1; }) {
    plan skip_all => 'cannot load NGCP::BulkProcessor::SqlConnectors::PostgreSQLDB (DBD::Pg missing?)';
}

my $dsn = NGCP::BulkProcessor::SqlConnectors::PostgreSQLDB::_dsn('cdr','db.example.org',5433);

like($dsn, qr/^dbi:Pg:database=cdr;host=db\.example\.org;port=5433;/,
    'dsn starts with database, host and port');

my %options = map { split(/=/, $_, 2); } split(/;/, substr($dsn, length('dbi:Pg:')));

# A silently vanished peer (host down, VIP failover, dropped firewall
# state) must not block a connect or a query forever:
cmp_ok($options{connect_timeout} // 0, '>', 0, 'connect_timeout set');
is($options{keepalives}, 1, 'tcp keepalives enabled');
cmp_ok($options{keepalives_idle} // 0, '>', 0, 'keepalives_idle set');
cmp_ok($options{keepalives_idle} // 0, '<=', 300, 'keepalives_idle well below the kernel default of 2h');
cmp_ok($options{keepalives_interval} // 0, '>', 0, 'keepalives_interval set');
cmp_ok($options{keepalives_count} // 0, '>', 0, 'keepalives_count set');
cmp_ok($options{tcp_user_timeout} // 0, '>', 0, 'tcp_user_timeout set');

done_testing();
