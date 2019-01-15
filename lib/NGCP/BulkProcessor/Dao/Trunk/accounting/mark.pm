package NGCP::BulkProcessor::Dao::Trunk::accounting::mark;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row

    insert_record
    update_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    get_system_mark
    get_reseller_mark

    set_system_mark
    set_reseller_mark

    insert_system_mark
    insert_reseller_mark

    cleanup_system_marks
    cleanup_reseller_marks
);

my $tablename = 'mark';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
"id",
"collector",
"acc_id",
];

my $indexes = {};

my $insert_unique_fields = [];

my $system_collector_format = '%s-lastseq';
my $reseller_collector_format = '%s-lastseq-%d';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub get_system_mark {

    my ($xa_db,$stream) = @_;

    return _get_mark($xa_db,sprintf($system_collector_format,$stream));

}

sub get_reseller_mark {

    my ($xa_db,$stream,$reseller_id) = @_;

    return _get_mark($xa_db,sprintf($reseller_collector_format,$stream,$reseller_id // ''));

}

sub _get_mark {

    my ($xa_db,$collector) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT MAX(' . $db->columnidentifier('acc_id') . ') FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('collector') . ' = ?';
    my @params = ($collector);

    my $mark = $xa_db->db_get_value($stmt,@params);

    return (defined $mark ? $mark : '0');

}

sub set_system_mark {

    my ($xa_db,$stream,$mark) = @_;

    return _set_mark($xa_db,sprintf($system_collector_format,$stream),$mark,0);

}

sub set_reseller_mark {

    my ($xa_db,$stream,$reseller_id,$mark) = @_;

    return _set_mark($xa_db,sprintf($reseller_collector_format,$stream,$reseller_id // ''),$mark,0);

}

sub insert_system_mark {

    my ($xa_db,$stream,$mark) = @_;

    return _set_mark($xa_db,sprintf($system_collector_format,$stream),$mark,1);

}

sub insert_reseller_mark {

    my ($xa_db,$stream,$reseller_id,$mark) = @_;

    return _set_mark($xa_db,sprintf($reseller_collector_format,$stream,$reseller_id // ''),$mark,1);

}

sub _set_mark {

    my ($xa_db,$collector,$mark,$force_insert) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $id;
    unless ($force_insert) {
        my $stmt = 'SELECT MAX(t1.id) FROM ' . $table . ' t1 LEFT JOIN ' . $table . ' t2' .
            ' ON t1.collector = t2.collector and t2.acc_id > t1.acc_id'.
            'WHERE t2.collector IS NULL AND t1.collector = ?';
        my @params = ($collector);
        $id = $xa_db->db_get_value($stmt,@params);
    }

    if (defined $id) {
        return update_record($get_db,$xa_db,__PACKAGE__,{
            id => $id,
            acc_id => $mark,
        });
    } else {
        if (insert_record($db,$xa_db,__PACKAGE__,{
                collector => $collector,
                acc_id => $mark
            },0,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
        return undef;
    }

}

sub cleanup_system_marks {

    my ($xa_db,$stream) = @_;

    return _cleanup_marks($xa_db,sprintf($system_collector_format,$stream));

}

sub cleanup_reseller_marks {

    my ($xa_db,$stream,$reseller_id) = @_;

    return _cleanup_marks($xa_db,sprintf($reseller_collector_format,$stream,$reseller_id // ''));

}

sub _cleanup_marks {

    my ($xa_db,$collector) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT MAX(t1.id) FROM ' . $table . ' t1 LEFT JOIN ' . $table . ' t2' .
        ' ON t1.collector = t2.collector and t2.acc_id > t1.acc_id'.
        'WHERE t2.collector IS NULL AND t1.collector = ?';
    my @params = ($collector);
    my $id = $xa_db->db_get_value($stmt,@params);

    if (defined $id) {
        $stmt = 'DELETE FROM ' . $table . ' WHERE collector = ? AND id != ?';
        push(@params,$id);
        if ($xa_db->db_do($stmt,@params)) {
            rowsdeleted($db,$tablename,1,1,getlogger(__PACKAGE__));
            return 1;
        } else {
            rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
            return 0;
        }
    }
    return 0;

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
