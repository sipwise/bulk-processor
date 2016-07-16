package NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers;
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
    insert_record
    update_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row

    findby_domainid_username_states
    countby_status_resellerid

    $TERMINATED_STATE
    $ACTIVE_STATE
);

my $tablename = 'voip_subscribers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'uuid',
    'username',
    'domain_id',
    'status',
    'primary_number_id',
    'external_id',
    'contact_id',
];

my $indexes = {};

my $insert_unique_fields = [];

our $TERMINATED_STATE = 'terminated';
our $ACTIVE_STATE = 'active';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_domainid_username_states {

    my ($xa_db,$domain_id,$username,$states,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('domain_id') . ' = ?' .
            ' AND ' . $db->columnidentifier('username') . ' = ?';
    my @params = ($domain_id,$username);
    if (defined $states and 'HASH' eq ref $states) {
        foreach my $in (keys %$states) {
            my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('status') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $states and length($states) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('status') . ' = ?';
        push(@params,$states);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_status_resellerid {

    my ($status,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' AS subscriber' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contracts::gettablename()) . ' AS contract ON subscriber.contract_id = contract.id' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contacts::gettablename()) . ' AS contact ON contract.contact_id = contact.id';
    my @params = ();
    my @terms = ();
    if ($status) {
        push(@terms,'subscriber.status = ?');
        push(@params,$status);
    }
    if ($reseller_id) {
        push(@terms,'contact.reseller_id = ?');
        push(@params,$reseller_id);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,$tablename,$data);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,$tablename,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($contract_id,
            $domain_id,
            $username,
            $uuid) = @params{qw/
                contract_id
                domain_id
                username
                uuid
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('contact_id') . ', ' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('domain_id') . ', ' .
                $db->columnidentifier('external_id') . ', ' .
                $db->columnidentifier('primary_number_id') . ', ' .
                $db->columnidentifier('status') . ', ' .
                $db->columnidentifier('username') . ', ' .
                $db->columnidentifier('uuid') . ') VALUES (' .
                'NULL, ' .
                '?, ' .
                '?, ' .
                'NULL, ' .
                'NULL, ' .
                '\'' . $ACTIVE_STATE . '\', ' .
                '?, ' .
                '?)',
                $contract_id,
                $domain_id,
                $username,
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
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
