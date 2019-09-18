package NGCP::BulkProcessor::Dao::mr553::billing::billing_mappings;
use strict;

use threads::shared;

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
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row

    findby_contractid_ts

    source_findby_contractid
);

my $tablename = 'billing_mappings';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'start_date',
    'end_date',
    'billing_profile_id',
    'contract_id',
    'product_id',
    'network_id',
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

sub findby_contractid_ts {

    my ($xa_db,$contract_id,$dt,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contract_id') . ' = ?';
    my @params = ($contract_id);
    if (defined $dt) {
        $stmt .= ' AND (' . $db->columnidentifier('start_date') . ' IS NULL OR ' . $db->columnidentifier('start_date') . ' <= ? ) ' .
            'AND (' . $db->columnidentifier('end_date') . ' IS NULL OR ' . $db->columnidentifier('end_date') . ' >= ? ) ' .
            'ORDER BY ' . $db->columnidentifier('start_date') . ' DESC, ' . $db->columnidentifier('id') . ' DESC LIMIT 1';
        push(@params, $db->datetime_to_string($dt) );
        push(@params, $db->datetime_to_string($dt) );
    }

    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

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
        my ($billing_profile_id,
            $contract_id,
            $product_id) = @params{qw/
                billing_profile_id
                contract_id
                product_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('billing_profile_id') . ', ' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('end_date') . ', ' .
                #$db->columnidentifier('network_id') . ', ' .
                $db->columnidentifier('product_id') . ', ' .
                $db->columnidentifier('start_date') . ') VALUES (' .
                '?, ' .
                '?, ' .
                'NULL, ' .
                #'NULL, ' .
                '?, ' .
                'NULL)',
                $billing_profile_id,
                $contract_id,
                $product_id,
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

    return checktableinfo(shift // $get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

sub source_new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new_shared($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findby_contractid {

    my ($source_dbs,$contract_id) = @_;

    my $source_db = $source_dbs->{billing_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT bm.*,p.class FROM ' . $table . ' bm JOIN ' .
            $db->tableidentifier('products') . ' p ON bm.product_id = p.id WHERE ' .
            'bm.contract_id = ?';
    my @params = ($contract_id);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{billing_db},$row);

            # transformations go here ...
            $record->{product_class} = $row->{class};

            push @records,$record;
        }
    }

    return \@records;

}

1;
