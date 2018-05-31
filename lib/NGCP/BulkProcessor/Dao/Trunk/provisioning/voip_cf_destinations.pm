package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_destinations;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db

);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
    insert_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    countby_subscriberid_type
    insert_row
);

my $tablename = 'voip_cf_destinations';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'destination_set_id',
  'destination',
  'priority',
  'timeout',
  #'announcement_id',
];

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub countby_subscriberid_type {

    my ($subscriber_id,$type,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($subscriber_id) {
        push(@terms,$db->columnidentifier('subscriber_id') . ' = ?');
        push(@params,$subscriber_id);
    }
    if ($type) {
        push(@terms,$db->columnidentifier('type') . ' = ?');
        push(@params,$type);
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
        my ($destination_set_id,
            $destination,
            $priority,
            $timeout,
            $announcement_id) = @params{qw/
                destination_set_id
                destination
                priority
                timeout
                announcement_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('destination_set_id') . ', ' .
                $db->columnidentifier('destination') . ', ' .
                $db->columnidentifier('priority') . ', ' .
                $db->columnidentifier('timeout') . ', ' .
                $db->columnidentifier('announcement_id') .') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                'NULL)'
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
