package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_trusted_sources;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row

    countby_subscriberid

    $PROTOCOL_UDP
    $PROTOCOL_TCP
    $PROTOCOL_TLS
    $PROTOCOL_ANY
);

my $tablename = 'voip_trusted_sources';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'subscriber_id',
  'src_ip',
  'protocol',
  'from_pattern',
  'uuid',

];

my $indexes = {};

my $insert_unique_fields = [];

our $PROTOCOL_UDP = 'UDP';
our $PROTOCOL_TCP = 'TCP';
our $PROTOCOL_TLS = 'TLS';
our $PROTOCOL_ANY = 'ANY';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub countby_subscriberid {

    my ($subscriber_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if (defined $subscriber_id) {
        push(@terms,$db->columnidentifier('subscriber_id') . ' = ?');
        push(@params,$subscriber_id);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($subscriber_id,
            $src_ip,
            $protocol,
            $from_pattern,
            $uuid) = @params{qw/
                subscriber_id
                src_ip
                protocol
                from_pattern
                uuid
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('subscriber_id') . ', ' .
                $db->columnidentifier('src_ip') . ', ' .
                $db->columnidentifier('protocol') . ', ' .
                $db->columnidentifier('from_pattern') . ', ' .
                $db->columnidentifier('uuid') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?)',
                $subscriber_id,
                $src_ip,
                $protocol,
                $from_pattern,
                $uuid,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...

            push @records,$record;
        }
    }

    return \@records;

}

sub gettablename {

    return $tablename;

}

sub check_table {

    return checktableinfo($get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
