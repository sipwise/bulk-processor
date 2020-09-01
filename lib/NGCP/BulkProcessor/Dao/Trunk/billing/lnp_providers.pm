package NGCP::BulkProcessor::Dao::Trunk::billing::lnp_providers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    copy_row
    insert_record
    update_record
    delete_record
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    
    insert_row
    update_row
    delete_row
    
    findby_prefix
    findby_nameprefixauthoritativeskiprewrite
    
    countby_name
);

my $tablename = 'lnp_providers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'name',
    'prefix',
    'authoritative',
    'skip_rewrite',
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

sub findby_prefix {

    my ($prefix,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($prefix) {
        push(@terms,$db->columnidentifier('prefix') . ' = ?');
        push(@params,$prefix);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_nameprefixauthoritativeskiprewrite {

    my ($name,$prefix,$authoritative,$skip_rewrite,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    push(@terms,$db->columnidentifier('name') . ' = ?');
    push(@params,$name);
    push(@terms,$db->columnidentifier('prefix') . ' = ?');
    push(@params,$prefix);
    push(@terms,$db->columnidentifier('authoritative') . ' = ?');
    push(@params,$authoritative);
    push(@terms,$db->columnidentifier('skip_rewrite') . ' = ?');
    push(@params,$skip_rewrite);
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_name {

    my ($name,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @terms = ();
    my @params = ();
    if (defined $name) {
        push(@terms,$db->columnidentifier('name') . ' = ?');
        push(@params,$name);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub delete_row {

    my ($xa_db,$data) = @_;

    check_table();
    return delete_record($get_db,$xa_db,__PACKAGE__,$data);

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
        my ($prefix,$name) = @params{qw/
                prefix
                name
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('prefix') . ', ' .
                $db->columnidentifier('name') . ') VALUES (' .
                '?, ?)',
                $prefix,$name,
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
