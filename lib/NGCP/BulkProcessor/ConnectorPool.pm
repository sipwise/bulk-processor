package NGCP::BulkProcessor::ConnectorPool;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(

    $accounting_databasename
    $accounting_username
    $accounting_password
    $accounting_host
    $accounting_port

    $billing_databasename
    $billing_username
    $billing_password
    $billing_host
    $billing_port

    $provisioning_databasename
    $provisioning_username
    $provisioning_password
    $provisioning_host
    $provisioning_port

    $kamailio_databasename
    $kamailio_username
    $kamailio_password
    $kamailio_host
    $kamailio_port

    $xa_databasename
    $xa_username
    $xa_password
    $xa_host
    $xa_port

    $ngcprestapi_uri
    $ngcprestapi_username
    $ngcprestapi_password
    $ngcprestapi_realm

);


use NGCP::BulkProcessor::Logging qw(getlogger);
use NGCP::BulkProcessor::LogError qw(dbclustererror dbclusterwarn); #nodumpdbset

use NGCP::BulkProcessor::SqlConnectors::MySQLDB;
#use NGCP::BulkProcessor::SqlConnectors::OracleDB;
#use NGCP::BulkProcessor::SqlConnectors::PostgreSQLDB;
#use NGCP::BulkProcessor::SqlConnectors::SQLiteDB qw($staticdbfilemode
#                              cleanupdbfiles);
#use NGCP::BulkProcessor::SqlConnectors::CSVDB;
#use NGCP::BulkProcessor::SqlConnectors::SQLServerDB;
use NGCP::BulkProcessor::RestConnectors::NGCPRestApi;

use NGCP::BulkProcessor::SqlProcessor qw(cleartableinfo);

use NGCP::BulkProcessor::Utils qw(threadid);

use NGCP::BulkProcessor::Array qw(
    filter
    mergearrays
    getroundrobinitem
    getrandomitem
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    get_accounting_db
    accounting_db_tableidentifier

    get_billing_db
    billing_db_tableidentifier

    get_provisioning_db
    provisioning_db_tableidentifier

    get_kamailio_db
    kamailio_db_tableidentifier

    get_xa_db
    xa_db_tableidentifier

    get_ngcp_restapi

    destroy_dbs
    get_connectorinstancename
    get_cluster_db
);

my $connectorinstancenameseparator = '_';

#my $logger = getlogger(__PACKAGE__);

# thread connector pools:
my $accounting_dbs = {};
my $billing_dbs = {};
my $provisioning_dbs = {};
my $kamailio_dbs = {};
my $xa_dbs = {};

my $ngcp_restapis = {};

sub get_accounting_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $accounting_dbs->{$name}) {
        $accounting_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $accounting_dbs->{$name}->db_connect($accounting_databasename,$accounting_username,$accounting_password,$accounting_host,$accounting_port);
    }
    return $accounting_dbs->{$name};

}

sub accounting_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$accounting_databasename));

}


sub get_billing_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $billing_dbs->{$name}) {
        $billing_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $billing_dbs->{$name}->db_connect($billing_databasename,$billing_username,$billing_password,$billing_host,$billing_port);
    }
    return $billing_dbs->{$name};

}

sub billing_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$billing_databasename));

}

sub get_provisioning_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $provisioning_dbs->{$name}) {
        $provisioning_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $provisioning_dbs->{$name}->db_connect($provisioning_databasename,$provisioning_username,$provisioning_password,$provisioning_host,$provisioning_port);
    }
    return $provisioning_dbs->{$name};

}

sub provisioning_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$provisioning_databasename));

}

sub get_kamailio_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $kamailio_dbs->{$name}) {
        $kamailio_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $kamailio_dbs->{$name}->db_connect($kamailio_databasename,$kamailio_username,$kamailio_password,$kamailio_host,$kamailio_port);
    }
    return $kamailio_dbs->{$name};

}

sub kamailio_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$kamailio_databasename));

}


sub get_xa_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $xa_dbs->{$name}) {
        $xa_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $xa_dbs->{$name}->db_connect($xa_databasename,$xa_username,$xa_password,$xa_host,$xa_port);
    }
    return $xa_dbs->{$name};

}

sub xa_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$xa_databasename));

}

sub get_ngcp_restapi {

    my ($instance_name) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $ngcp_restapis->{$name}) {
        $ngcp_restapis->{$name} = NGCP::BulkProcessor::RestConnectors::NGCPRestApi->new($instance_name);
        $ngcp_restapis->{$name}->setup($ngcprestapi_uri,$ngcprestapi_username,$ngcprestapi_password,$ngcprestapi_realm);
    }
    return $ngcp_restapis->{$name};

}


sub get_connectorinstancename {
    my ($name) = @_;
    my $instance_name = threadid();
    if (length($name) > 0) {
        $instance_name .= $connectorinstancenameseparator . $name;
    }
    return $instance_name;
}

sub destroy_dbs {


    foreach my $name (keys %$accounting_dbs) {
        cleartableinfo($accounting_dbs->{$name});
        undef $accounting_dbs->{$name};
        delete $accounting_dbs->{$name};
    }

    foreach my $name (keys %$billing_dbs) {
        cleartableinfo($billing_dbs->{$name});
        undef $billing_dbs->{$name};
        delete $billing_dbs->{$name};
    }

    foreach my $name (keys %$provisioning_dbs) {
        cleartableinfo($provisioning_dbs->{$name});
        undef $provisioning_dbs->{$name};
        delete $provisioning_dbs->{$name};
    }

    foreach my $name (keys %$kamailio_dbs) {
        cleartableinfo($kamailio_dbs->{$name});
        undef $kamailio_dbs->{$name};
        delete $kamailio_dbs->{$name};
    }

    foreach my $name (keys %$xa_dbs) {
        cleartableinfo($xa_dbs->{$name});
        undef $xa_dbs->{$name};
        delete $xa_dbs->{$name};
    }

}

sub get_cluster_db { # oracle RAC and the like ...

    my ($cluster,$instance_name,$reconnect) = @_;
    #if ((defined $cluster) and ref $cluster ne 'HASH') {
        my $node = undef;
        my $tid = threadid();
        if ((!defined $cluster->{scheduling_vars}) or ref $cluster->{scheduling_vars} ne 'HASH') {
            $cluster->{scheduling_vars} = {};
        }
        my $scheduling_vars = $cluster->{scheduling_vars};
        if ((!defined $scheduling_vars->{$tid}) or ref $scheduling_vars->{$tid} ne 'HASH') {
            $scheduling_vars->{$tid} = {};
        }
        $scheduling_vars = $scheduling_vars->{$tid};
        my $nodes;
        if (!defined $scheduling_vars->{nodes}) {
            $nodes = {};
            foreach my $node (@{$cluster->{nodes}}) {
                if (defined $node and ref $node eq 'HASH') {
                    if ($node->{active}) {
                        $nodes->{$node->{label}} = $node;
                    }
                } else {
                    dbclustererror($cluster->{name},'node configuration error',getlogger(__PACKAGE__));
                }
            }
            $scheduling_vars->{nodes} = $nodes;
        } else {
            $nodes = $scheduling_vars->{nodes};
        }
        my @active_nodes = @{$nodes}{sort keys(%$nodes)}; #hash slice
        if (defined $cluster->{scheduling_code} and ref $cluster->{scheduling_code} eq 'CODE') {
            my $cluster_instance_name;
            if (length($instance_name) > 0) {
                $cluster_instance_name = $cluster->{name} . $connectorinstancenameseparator . $instance_name;
            } else {
                $cluster_instance_name = $cluster->{name};
            }
            ($node,$scheduling_vars->{node_index}) = &{$cluster->{scheduling_code}}(\@active_nodes,$scheduling_vars->{node_index});
            if (defined $node) {
                my $get_db = $node->{get_db};
                if (defined $get_db and ref $get_db eq 'CODE') {
                    my $db = undef;
                    eval {
                        $db = &{$get_db}($cluster_instance_name,$reconnect,$cluster);
                    };
                    if ($@) {
                        dbclusterwarn($cluster->{name},'node ' . $node->{label} . ' inactive',getlogger(__PACKAGE__));
                        delete $nodes->{$node->{label}};
                        return get_cluster_db($cluster,$instance_name,$reconnect);
                    } else {
                        #$db->cluster($cluster);
                        return $db;
                    }
                } else {
                    dbclustererror($cluster->{name},'node ' . $node->{label} . ' configuration error',getlogger(__PACKAGE__));
                    delete $nodes->{$node->{label}};
                    return get_cluster_db($cluster,$instance_name,$reconnect);
                }
            }
        } else {
            dbclustererror($cluster->{name},'scheduling configuration error',getlogger(__PACKAGE__));
            return undef;
        }

    #}
    dbclustererror($cluster->{name},'cannot switch to next active node',getlogger(__PACKAGE__));
    return undef;

}

1;
