package Projects::Migration::IPGallery::ProjectConnectorPool;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use Projects::Migration::IPGallery::Settings qw(
    $import_db_file
);

use ConnectorPool qw(
    get_connectorinstancename
);

#use SqlConnectors::MySQLDB;
#use SqlConnectors::OracleDB;
#use SqlConnectors::PostgreSQLDB;
use SqlConnectors::SQLiteDB qw(
    $staticdbfilemode
    cleanupdbfiles
);
#use SqlConnectors::CSVDB;
#use SqlConnectors::SQLServerDB;
#use RestConnectors::NGCPRestApi;

use SqlRecord qw(cleartableinfo);

#use Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    get_import_db
    import_db_tableidentifier

    destroy_dbs
);

# thread connector pools:
my $import_dbs = {};


sub get_import_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name); #threadid(); #shift;

    if (not defined $import_dbs->{$name}) {
        $import_dbs->{$name} = SqlConnectors::SQLiteDB->new($instance_name); #$name);
        if (not defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $import_dbs->{$name}->db_connect($staticdbfilemode,$import_db_file);
    }

    return $import_dbs->{$name};

}

sub import_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(SqlConnectors::SQLiteDB::get_tableidentifier($tablename,$staticdbfilemode,$import_db_file));

}


sub destroy_dbs {


    foreach my $name (keys %$import_dbs) {
        cleartableinfo($import_dbs->{$name});
        undef $import_dbs->{$name};
        delete $import_dbs->{$name};
    }

}

1;
