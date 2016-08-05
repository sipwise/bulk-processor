package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    get_import_db

);
#import_db_tableidentifier

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    getupsertstatement

    findby_fqdn
    countby_fqdn

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $unchanged_delta
    $updated_delta
    $added_delta

);

my $tablename = 'user_password';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [
    'fqdn',
    'username',
    'password',
    #'auth_level',
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'fqdn' ];
my $indexes = {
    $tablename . '_username_password' => [ 'username(11)', 'password(32)' ],
    $tablename . '_delta' => [ 'delta(7)' ],
};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
our $unchanged_delta = 'UNCHANGED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

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

sub findby_fqdn {

    my ($fqdn,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('fqdn') . ' = ?'
    ,$fqdn);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub update_delta {

    my ($fqdn,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $fqdn) {
        $stmt .= ' WHERE ' . $db->columnidentifier('fqdn') . ' = ?';
        push(@params,$fqdn);
    }

    return $db->db_do($stmt,@params);

}

sub countby_fqdn {

    my ($fqdn) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $fqdn) {
        $stmt .= ' WHERE ' . $db->columnidentifier('fqdn') . ' = ?';
        push(@params,$fqdn);
    }

    return $db->db_get_value($stmt,@params);

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
            my $unchanged_stmt = 'SELECT \'' . $unchanged_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('fqdn') . ' = ? AND ' . $db->columnidentifier('password') . ' = ?';
            my $updated_stmt = 'SELECT \'' . $updated_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('fqdn') . ' = ?';
            push(@values,'COALESCE((' . $unchanged_stmt . '), (' . $updated_stmt . '), \'' . $added_delta . '\')');
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
