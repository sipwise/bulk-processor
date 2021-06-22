package NGCP::BulkProcessor::Projects::ETL::Customer::ProjectConnectorPool;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use NGCP::BulkProcessor::Projects::ETL::Customer::Settings qw(

    $sqlite_db_file
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_connectorinstancename
);

use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw($staticdbfilemode);

use NGCP::BulkProcessor::SqlProcessor qw(cleartableinfo);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

    get_sqlite_db
    sqlite_db_tableidentifier

    destroy_dbs
    destroy_all_dbs
    ping_all_dbs

);

my $sqlite_dbs = {};

sub get_sqlite_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name); #threadid(); #shift;

    if (not defined $sqlite_dbs->{$name}) {
        $sqlite_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::SQLiteDB->new($instance_name); #$name);
        if (not defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $sqlite_dbs->{$name}->db_connect($staticdbfilemode,$sqlite_db_file);
    }

    return $sqlite_dbs->{$name};

}

sub sqlite_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::SQLiteDB::get_tableidentifier($tablename,$staticdbfilemode,$sqlite_db_file));

}

sub destroy_dbs {

    foreach my $name (keys %$sqlite_dbs) {
        cleartableinfo($sqlite_dbs->{$name});
        undef $sqlite_dbs->{$name};
        delete $sqlite_dbs->{$name};
    }

}

sub destroy_all_dbs() {
    destroy_dbs();
    NGCP::BulkProcessor::ConnectorPool::destroy_dbs();
}

sub ping_all_dbs() {
    NGCP::BulkProcessor::ConnectorPool::ping_dbs();
}

1;
