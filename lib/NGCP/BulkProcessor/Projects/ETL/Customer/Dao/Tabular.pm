package NGCP::BulkProcessor::Projects::ETL::Customer::Dao::Tabular;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::ETL::Customer::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::Projects::ETL::Customer::Settings qw(
    $tabular_fields
);

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row
    insert_stmt
    transfer_table
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

    get_fieldnames

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta
    
    copy_table
);

my $tablename = 'tabular';
my $get_db = \&get_sqlite_db;

my $fieldnames;
my $expected_fieldnames;
sub get_fieldnames {
    my $expected = shift;
    unless (defined $fieldnames and defined $expected_fieldnames) {
        $fieldnames = [ map {
            local $_ = (ref $_ ? $_->{path} : $_);
            $_ =~ s/\./_/g;
            $_ =~ s/\[(\d+)\]/_$1/g;
            $_;
        } @$tabular_fields ];
        $expected_fieldnames = [ @$fieldnames ];
        push(@$expected_fieldnames,'uuid') unless grep { 'uuid' eq $_; } @$expected_fieldnames;
        push(@$expected_fieldnames,'delta');
    }
    return $fieldnames unless $expected;
    return $expected_fieldnames;
}

my $primarykey_fieldnames = [ 'uuid' ];
my $indexes = {
    $tablename . '_delta' => [ 'delta(7)' ],
};

our $deleted_delta = 'DELETED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,get_fieldnames(1),$indexes);

    copy_row($self,shift,get_fieldnames(1));

    return $self;

}

sub create_table {

    my ($truncate) = @_;

    my $db = &$get_db();

    registertableinfo($db,__PACKAGE__,$tablename,get_fieldnames(1),$indexes,$primarykey_fieldnames);
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
    , $delta);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_domainusername {

    my ($domain,$username,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless (defined $domain and defined $username);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' . $table .
        ' WHERE ' . $db->columnidentifier('domain') . ' = ?' .
        ' AND ' . $db->columnidentifier('username') . ' = ?'
    , $domain, $username);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub update_delta {

    my ($uuid,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $uuid) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('uuid') . ' = ?';
        push(@params, $uuid);
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

sub copy_table {
    
    my ($get_target_db) = @_;
     
    check_table();
    #checktableinfo($get_target_db,
    #    __PACKAGE__,$tablename,
    #    get_fieldnames(1),
    #    $indexes);

    return transfer_table(
        get_db => $get_db,
        class => __PACKAGE__,
        get_target_db => $get_target_db,
        targetclass => __PACKAGE__,
        targettablename => $tablename,
    );
    
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
      join(', ', map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @{get_fieldnames(1)}) . ')';
    my @values = ();
    foreach my $fieldname (@{get_fieldnames(1)}) {
        if ('delta' eq $fieldname) {
            my $stmt = 'SELECT \'' . $updated_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('uuid') . ' = ?';
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
                   get_fieldnames(1),
                   $indexes);

}

1;
