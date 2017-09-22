package NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::CallForward;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);
#import_db_tableidentifier

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt


);
#process_table
use NGCP::BulkProcessor::SqlRecord qw();

#use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    getupsertstatement

    @fieldnames

    findby_ccacsntypedestination
    findby_sipusername
    countby_ccacsntype

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta

);
#findby_sipusername
#countby_clir

my $tablename = 'callforward';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;

our @fieldnames = (
    "cc",
    "ac",
    "sn",
    "type",
    "destination",
    "priority",
    "timeout",
    "ringtimeout",

    #calculated fields at the end!
    "sip_username",
    'rownum',
    'filename',
);

my $expected_fieldnames = [
    @fieldnames,
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ "cc", "ac", "sn", "type", "destination" ];
my $indexes = {
    $tablename . '_rownum' => [ 'rownum(11)' ],
    $tablename . '_delta' => [ 'delta(7)' ],
};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub create_table {

    my ($truncate) = @_;

    my $db = &$get_db();

    registertableinfo($db,__PACKAGE__,$tablename,$expected_fieldnames,$indexes,$primarykey_fieldnames);
    return create_targettable($db,__PACKAGE__,$db,__PACKAGE__,$tablename,$truncate,0,undef);

}

sub findby_delta {

    my ($delta,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless defined $delta;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('delta') . ' = ?'
    ,$delta);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_ccacsntypedestination {

    my ($cc,$ac,$sn,$type,$destination,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless (defined $cc or defined $ac or defined $sn or defined $type or defined $destination);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?' .
            ' AND ' . $db->columnidentifier('type') . ' = ?' .
            ' AND ' . $db->columnidentifier('destination') . ' = ?'
    ,$cc,$ac,$sn,$type,$destination);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub findby_sipusername {

    my ($sip_username,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    #return [] unless (defined $cc or defined $ac or defined $sn);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('sip_username') . ' = ?'
    ,$sip_username);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_ccacsntype {

    my ($cc,$ac,$sn,$type) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if (defined $cc or defined $ac or defined $sn) {
        push(@terms,
            $db->columnidentifier('cc') . ' = ?',
            $db->columnidentifier('ac') . ' = ?',
            $db->columnidentifier('sn') . ' = ?');
        push(@params,$cc,$ac,$sn);
    }
    if (defined $type) {
        push(@terms,
            $db->columnidentifier('type') . ' = ?');
        push(@params,$type);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_delta {

    my ($cc,$ac,$sn,$type,$destination,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $cc or defined $ac or defined $sn or defined $type or defined $destination) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?' .
            ' AND ' . $db->columnidentifier('type') . ' = ?' .
            ' AND ' . $db->columnidentifier('destination') . ' = ?';
        push(@params,$cc,$ac,$sn,$type,$destination);
    }

    return $db->db_do($stmt,@params);

}


sub countby_delta {

    my ($deltas) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' WHERE 1=1';
    my @params = ();
    if (defined $deltas and 'HASH' eq ref $deltas) {
        foreach my $in (keys %$deltas) {
            my @values = (defined $deltas->{$in} and 'ARRAY' eq ref $deltas->{$in} ? @{$deltas->{$in}} : ($deltas->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('delta') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $deltas and length($deltas) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('delta') . ' = ?';
        push(@params,$deltas);
    }

    return $db->db_get_value($stmt,@params);

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


sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,__PACKAGE__,$insert_ignore);

}

sub getupsertstatement {

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);
    my $upsert_stmt = 'INSERT OR REPLACE INTO ' . $table . ' (' .
      join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @$expected_fieldnames) . ')';
    my @values = ();
    foreach my $fieldname (@$expected_fieldnames) {
        if ('delta' eq $fieldname) {
            my $stmt = 'SELECT \'' . $updated_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('cc') . ' = ?' .
                ' AND ' . $db->columnidentifier('ac') . ' = ?' .
                ' AND ' . $db->columnidentifier('sn') . ' = ?' .
                ' AND ' . $db->columnidentifier('type') . ' = ?' .
                ' AND ' . $db->columnidentifier('destination') . ' = ?';
            push(@values,'COALESCE((' . $stmt . '), \'' . $added_delta . '\')');
        } else {
            push(@values,'?');
        }
    }
    $upsert_stmt .= ' VALUES (' . join(',',@values) . ')';
    return $upsert_stmt;

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
