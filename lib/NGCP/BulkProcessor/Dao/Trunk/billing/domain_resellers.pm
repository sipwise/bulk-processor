package NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db

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

    insert_row

    countby_domainid_resellerid
    findby_domainid_resellerid
);

my $tablename = 'domain_resellers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'domain_id',
    'reseller_id',
];

my $indexes = {};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub countby_domainid_resellerid {

    my ($domain_id,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($domain_id) {
        push(@terms,$db->columnidentifier('domain_id') . ' = ?');
        push(@params,$domain_id);
    }
    if ($reseller_id) {
        push(@terms,$db->columnidentifier('reseller_id') . ' = ?');
        push(@params,$reseller_id);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub findby_domainid_resellerid {

    my ($domain_id,$reseller_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($domain_id) {
        push(@terms,$db->columnidentifier('domain_id') . ' = ?');
        push(@params,$domain_id);
    }
    if ($reseller_id) {
        push(@terms,$db->columnidentifier('reseller_id') . ' = ?');
        push(@params,$reseller_id);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

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
        my ($domain_id,
            $reseller_id) = @params{qw/
                domain_id
                reseller_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('domain_id') . ', ' .
                $db->columnidentifier('reseller_id') . ') VALUES (' .
                '?, ' .
                '?)',
                $domain_id,
                $reseller_id,
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
            #if ($load_recursive and $load_recursive->{domain}) {
            #    $record->{domain} = NGCP::BulkProcessor::Dao::Trunk::billing::domains::findby_id($record->{domain_id},$load_recursive);
            #}
            #if ($load_recursive and $load_recursive->{reseller}) {
            #    $record->{reseller} = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_id($record->{reseller_id},$load_recursive);
            #}

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
