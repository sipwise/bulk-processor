package NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::ProjectConnectorPool;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../');

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Settings qw(
    $source_accounting_databasename
    $source_accounting_username
    $source_accounting_password
    $source_accounting_host
    $source_accounting_port

    $source_billing_databasename
    $source_billing_username
    $source_billing_password
    $source_billing_host
    $source_billing_port

    $source_provisioning_databasename
    $source_provisioning_username
    $source_provisioning_password
    $source_provisioning_host
    $source_provisioning_port

    $source_kamailio_databasename
    $source_kamailio_username
    $source_kamailio_password
    $source_kamailio_host
    $source_kamailio_port

    $source_rowblock_transactional
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_connectorinstancename
    ping
);

use NGCP::BulkProcessor::SqlConnectors::MySQLDB;
#use NGCP::BulkProcessor::RestConnectors::NGCPRestApi;

use NGCP::BulkProcessor::SqlProcessor qw(cleartableinfo);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    get_source_accounting_db
    source_accounting_db_tableidentifier

    get_source_billing_db
    source_billing_db_tableidentifier

    get_source_provisioning_db
    source_provisioning_db_tableidentifier

    get_source_kamailio_db
    source_kamailio_db_tableidentifier

    destroy_dbs
    destroy_all_dbs

    ping_dbs
    ping_all_dbs
);

my $source_accounting_dbs = {};
my $source_billing_dbs = {};
my $source_provisioning_dbs = {};
my $source_kamailio_dbs = {};

sub get_source_accounting_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $source_accounting_dbs->{$name}) {
        $source_accounting_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($source_rowblock_transactional,$instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $source_accounting_dbs->{$name}->db_connect($source_accounting_databasename,$source_accounting_username,$source_accounting_password,$source_accounting_host,$source_accounting_port);
    }
    return $source_accounting_dbs->{$name};

}

sub source_accounting_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$source_accounting_databasename));

}


sub get_source_billing_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $source_billing_dbs->{$name}) {
        $source_billing_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($source_rowblock_transactional,$instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $source_billing_dbs->{$name}->db_connect($source_billing_databasename,$source_billing_username,$source_billing_password,$source_billing_host,$source_billing_port);
    }
    return $source_billing_dbs->{$name};

}

sub source_billing_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$source_billing_databasename));

}

sub get_source_provisioning_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $source_provisioning_dbs->{$name}) {
        $source_provisioning_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($source_rowblock_transactional,$instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $source_provisioning_dbs->{$name}->db_connect($source_provisioning_databasename,$source_provisioning_username,$source_provisioning_password,$source_provisioning_host,$source_provisioning_port);
    }
    return $source_provisioning_dbs->{$name};

}

sub source_provisioning_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$source_provisioning_databasename));

}

sub get_source_kamailio_db {

    my ($instance_name,$reconnect) = @_;
    my $name = get_connectorinstancename($instance_name);
    if (!defined $source_kamailio_dbs->{$name}) {
        $source_kamailio_dbs->{$name} = NGCP::BulkProcessor::SqlConnectors::MySQLDB->new($source_rowblock_transactional,$instance_name);
        if (!defined $reconnect) {
            $reconnect = 1;
        }
    }
    if ($reconnect) {
        $source_kamailio_dbs->{$name}->db_connect($source_kamailio_databasename,$source_kamailio_username,$source_kamailio_password,$source_kamailio_host,$source_kamailio_port);
    }
    return $source_kamailio_dbs->{$name};

}

sub source_kamailio_db_tableidentifier {

    my ($get_target_db,$tablename) = @_;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;
    return $target_db->getsafetablename(NGCP::BulkProcessor::SqlConnectors::MySQLDB::get_tableidentifier($tablename,$source_kamailio_databasename));

}

sub ping_dbs {
    ping($source_accounting_dbs);
    ping($source_billing_dbs);
    ping($source_provisioning_dbs);
    ping($source_kamailio_dbs);
}

sub ping_all_dbs {
    ping_dbs();
    NGCP::BulkProcessor::ConnectorPool::ping_dbs();
}

sub destroy_dbs {

    foreach my $name (keys %$source_accounting_dbs) {
        cleartableinfo($source_accounting_dbs->{$name});
        undef $source_accounting_dbs->{$name};
        delete $source_accounting_dbs->{$name};
    }

    foreach my $name (keys %$source_billing_dbs) {
        cleartableinfo($source_billing_dbs->{$name});
        undef $source_billing_dbs->{$name};
        delete $source_billing_dbs->{$name};
    }

    foreach my $name (keys %$source_provisioning_dbs) {
        cleartableinfo($source_provisioning_dbs->{$name});
        undef $source_provisioning_dbs->{$name};
        delete $source_provisioning_dbs->{$name};
    }

    foreach my $name (keys %$source_kamailio_dbs) {
        cleartableinfo($source_kamailio_dbs->{$name});
        undef $source_kamailio_dbs->{$name};
        delete $source_kamailio_dbs->{$name};
    }

}

sub destroy_all_dbs() {
    destroy_dbs();
    NGCP::BulkProcessor::ConnectorPool::destroy_dbs();
}

1;
