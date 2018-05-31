use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../');

# mysql, oracle, mssql, .. matrix db interconnection test

use NGCP::BulkProcessor::Globals qw($defaultconfig);
use NGCP::BulkProcessor::LoadConfig qw(
    load_config
);

use NGCP::BulkProcessor::ConnectorPool qw(
    destroy_dbs
    get_sqlserver_test_db
    get_postgres_test_db
    get_oracle_test_db
    get_mysql_test_db
    get_csv_test_db
    get_sqlite_test_db
);

use Test::Unit::Procedural;

use test::csv_table;
use test::mysql_table;
use test::oracle_table;
use test::postgres_table;
use test::sqlite_table;
use test::sqlserver_table;

load_config($defaultconfig);

my $sort_config = [ {  numeric     => 1,
                       dir => 1,
                       column => 'column1',
                    },
                    {  numeric     => 1,
                       dir => -1,
                       memberchain => 'column2',
                    },
                   ];

sub set_up {


}


sub test_sync_tables_to_sqlite {

  test::sqlserver_table::sync_table(\&get_sqlite_test_db);
  test::postgres_table::sync_table(\&get_sqlite_test_db);
  test::oracle_table::sync_table(\&get_sqlite_test_db);
  test::sqlite_table::sync_table(\&get_sqlite_test_db);
  test::csv_table::sync_table(\&get_sqlite_test_db);
  test::mysql_table::sync_table(\&get_sqlite_test_db);

}

sub test_sync_tables_to_mysql {

  test::sqlserver_table::sync_table(\&get_mysql_test_db);
  test::postgres_table::sync_table(\&get_mysql_test_db);
  test::oracle_table::sync_table(\&get_mysql_test_db);
  test::sqlite_table::sync_table(\&get_mysql_test_db);
  test::csv_table::sync_table(\&get_mysql_test_db);
  test::mysql_table::sync_table(\&get_mysql_test_db);

}

sub test_sync_tables_to_postgres {

  test::sqlserver_table::sync_table(\&get_postgres_test_db);
  test::postgres_table::sync_table(\&get_postgres_test_db);
  test::oracle_table::sync_table(\&get_postgres_test_db);
  test::sqlite_table::sync_table(\&get_postgres_test_db);
  test::csv_table::sync_table(\&get_postgres_test_db);
  test::mysql_table::sync_table(\&get_postgres_test_db);

}

sub test_sync_tables_to_oracle {

  test::sqlserver_table::sync_table(\&get_oracle_test_db);
  test::postgres_table::sync_table(\&get_oracle_test_db);
  test::oracle_table::sync_table(\&get_oracle_test_db);
  test::sqlite_table::sync_table(\&get_oracle_test_db);
  test::csv_table::sync_table(\&get_oracle_test_db);
  test::mysql_table::sync_table(\&get_oracle_test_db);

}

sub test_sync_tables_to_sqlserver {

  test::sqlserver_table::sync_table(\&get_sqlserver_test_db);
  test::postgres_table::sync_table(\&get_sqlserver_test_db);
  test::oracle_table::sync_table(\&get_sqlserver_test_db);
  test::sqlite_table::sync_table(\&get_sqlserver_test_db);
  test::csv_table::sync_table(\&get_sqlserver_test_db);
  test::mysql_table::sync_table(\&get_sqlserver_test_db);

}

sub test_sync_tables_to_csv {

  test::sqlserver_table::sync_table(\&get_csv_test_db);
  test::postgres_table::sync_table(\&get_csv_test_db);
  test::oracle_table::sync_table(\&get_csv_test_db);
  test::sqlite_table::sync_table(\&get_csv_test_db);
  test::csv_table::sync_table(\&get_csv_test_db);
  test::mysql_table::sync_table(\&get_csv_test_db);

}

sub test_select_source_sqlserver {

    my $result = test::sqlserver_table::test_table_source_select('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_postgres {

    my $result = test::postgres_table::test_table_source_select('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_oracle {

    my $result = test::oracle_table::test_table_source_select('column1 is not null',0,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_sqlite {

    my $result = test::sqlite_table::test_table_source_select('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_csv {

    my $result = test::csv_table::test_table_source_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_mysql {

    my $result = test::mysql_table::test_table_source_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_local {
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'sqlserver';
    _table_local_selects();
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'postgres';
    _table_local_selects();
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'oracle';
    _table_local_selects();
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'sqlite';
    _table_local_selects();
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'mysql';
    _table_local_selects();
    $NGCP::BulkProcessor::ConnectorPool::test_db = 'csv';
    _table_local_selects();

    $NGCP::BulkProcessor::ConnectorPool::test_db = 'mysql';
}
sub _table_local_selects {
    my $result = test::sqlserver_table::test_table_local_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'local query failed');
    $result = test::postgres_table::test_table_local_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');
    $result = test::oracle_table::test_table_local_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'local query failed');
    $result = test::sqlite_table::test_table_local_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');
    $result = test::csv_table::test_table_local_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');
    $result = test::mysql_table::test_table_source_select('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');
}

sub test_select_source_temp_sqlserver {

    my $result = test::sqlserver_table::test_table_source_select_temptable('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_temp_postgres {

    my $result = test::postgres_table::test_table_source_select_temptable('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub test_select_source_temp_oracle {

    for (my $i = 0; $i < 10; $i++) {
        my $result = test::oracle_table::test_table_source_select_temptable('column1 is not null',0,1,$sort_config);
        assert((scalar @$result) == 1,'source query failed');
    }

}

sub test_select_source_temp_sqlite {

    my $result = test::sqlite_table::test_table_source_select_temptable('column1 is not null',2,3,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

#not supported
#sub test_select_source_temp_csv {
#
#
#}

sub test_select_source_temp_mysql {

    my $result = test::mysql_table::test_table_source_select_temptable('column1 is not null',2,1,$sort_config);
    assert((scalar @$result) == 1,'source query failed');

}

sub tear_down {

}

create_suite();
run_suite();

destroy_dbs();

exit;
