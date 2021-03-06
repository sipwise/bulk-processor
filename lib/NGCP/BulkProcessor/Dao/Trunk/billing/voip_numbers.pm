package NGCP::BulkProcessor::Dao::Trunk::billing::voip_numbers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowupdated
    rowupdateskipped
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row

    findby_id
    findby_subscriberid
    forupdate_cc_ac_sn_subscriberid
    release_subscriber_numbers
    findby_cc_ac_sn

    countby_ccacsn

    $ACTIVE_STATE
);

my $tablename = 'voip_numbers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'cc',
    'ac',
    'sn',
    'reseller_id',
    'subscriber_id',
    'status',
    'ported',
    'list_timestamp',
];

my $indexes = {};

my $insert_unique_fields = [];

our $ACTIVE_STATE = 'active';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_id {

    my ($id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('id') . ' = ?';
    my @params = ($id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findby_subscriberid {

    my ($xa_db,$subscriber_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
        $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}


sub countby_ccacsn {

    my ($xa_db,$cc,$ac,$sn) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?';
    my @params = ($cc // '',$ac // '',$sn // '');
    return $db->db_get_value($stmt,@params);

}

sub forupdate_cc_ac_sn_subscriberid {

    my ($xa_db,$cc,$ac,$sn,$subscriber_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?' .
            ' AND (' . $db->columnidentifier('subscriber_id') . ' = ? OR ' . $db->columnidentifier('subscriber_id') . ' IS NULL)' .
            ' FOR UPDATE';
    my @params = ($cc,$ac,$sn,$subscriber_id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

    #my $stmt = $db->paginate_sort_query('SELECT ' . $db->columnidentifier('id') . ' FROM ' . $table . ' WHERE ' .
    #        $db->columnidentifier('cc') . ' = ?' .
    #        ' AND ' . $db->columnidentifier('ac') . ' = ?' .
    #        ' AND ' . $db->columnidentifier('sn') . ' = ?' .
    #        ' AND (' . $db->columnidentifier('subscriber_id') . ' = ? OR ' . $db->columnidentifier('subscriber_id') . ' IS NULL)',undef,undef,[{
    #        column => 'id',
    #        numeric => 1,
    #        dir => 1,
    #    }]);
    #my @params = ($cc,$ac,$sn,$subscriber_id);
    #foreach my $id (@{$xa_db->db_get_col($stmt,@params)}) {
    #    return buildrecords_fromrows([
    #        $xa_db->db_get_row('SELECT * FROM ' . $table . ' WHERE ' . $db->columnidentifier('id') . ' = ? FOR UPDATE',$id)
    #    ],$load_recursive)->[0];
    #}
    #return undef;

}

sub findby_cc_ac_sn {

    my ($xa_db,$cc,$ac,$sn,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?';
    my @params = ($cc,$ac,$sn);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub release_subscriber_numbers {

    my ($xa_db,$subscriber_id,$ids) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET ' .
            $db->columnidentifier('reseller_id') . ' = NULL, ' .
            $db->columnidentifier('subscriber_id') . ' = NULL WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $ids and 'HASH' eq ref $ids) {
        foreach my $in (keys %$ids) {
            my @values = (defined $ids->{$in} and 'ARRAY' eq ref $ids->{$in} ? @{$ids->{$in}} : ($ids->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('id') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $ids and length($ids) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('id') . ' != ?';
        push(@params,$ids);
    }

    if ($xa_db->db_do($stmt,@params)) {
        rowupdated($db,$tablename,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowupdateskipped($db,$tablename,0,getlogger(__PACKAGE__));
        return 0;
    }

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

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
        my ($ac,
            $cc,
            $reseller_id,
            $sn,
            $subscriber_id) = @params{qw/
                ac
                cc
                reseller_id
                sn
                subscriber_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('ac') . ', ' .
                $db->columnidentifier('cc') . ', ' .
                $db->columnidentifier('reseller_id') . ', ' .
                $db->columnidentifier('sn') . ', ' .
                $db->columnidentifier('status') . ', ' .
                $db->columnidentifier('subscriber_id') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '\'' . $ACTIVE_STATE . '\', ' .
                '?)',
                $ac,
                $cc,
                $reseller_id,
                $sn,
                $subscriber_id,
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
