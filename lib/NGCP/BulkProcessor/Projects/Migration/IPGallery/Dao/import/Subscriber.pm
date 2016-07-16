package NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
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

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption qw();
use NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    getupsertstatement

    findby_subscribernumber
    countby_subscribernumber
    update_delta
    findby_delta
    countby_delta

    split_subscribernumber

    $deleted_delta
    $updated_delta
    $added_delta

    process_records
);

my $tablename = 'subscriber';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;


my $expected_fieldnames = [
    'country_code', #356
    'area_code', #None
    'dial_number', #35627883323
    'rgw_fqdn', #35627883323
    'port', #None
    'region_name', #None
    'carrier_code', #None
    'time_zone_name', #malta
    'lang_code', #eng
    'barring_profile', #None
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'country_code', 'area_code', 'dial_number' ];
my $indexes = { $tablename . '_delta' => [ 'delta(7)' ]};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
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

    registertableinfo($db,$tablename,$expected_fieldnames,$indexes,$primarykey_fieldnames);
    return create_targettable($db,$tablename,$db,$tablename,$truncate,0,undef);

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

sub findby_subscribernumber {

    my ($subscribernumber,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless defined $subscribernumber;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('country_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('dial_number') . ' = ?'
    ,split_subscribernumber($subscribernumber));

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub countby_subscribernumber {

    my ($subscribernumber) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $subscribernumber) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('country_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('dial_number') . ' = ?';
        push(@params,split_subscribernumber($subscribernumber));
    }

    return $db->db_get_value($stmt,@params);

}

sub countby_delta {

    my ($delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $delta) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('delta') . ' = ?';
        push(@params,$delta);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_delta {

    my ($subscribernumber,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $subscribernumber) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('country_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
            ' AND ' . $db->columnidentifier('dial_number') . ' = ?';
        push(@params,split_subscribernumber($subscribernumber));
    }

    return $db->db_do($stmt,@params);

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...
            if ($load_recursive) {
                $record->{_features} = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::findby_subscribernumber_option(
                    $record->subscribernumber(),
                    undef,
                    $load_recursive,
                );
                $record->{_userpassword} = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::UsernamePassword::findby_fqdn(
                    $record->subscribernumber(), #$record->{rgw_fqdn}
                    $load_recursive,
                );
            }

            push @records,$record;
        }
    }

    return \@records;

}

sub process_records {

    my %params = @_;
    my ($process_code,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            load_recursive
        /};

    check_table();

    return process_table(
        get_db                      => $get_db,
        tablename                   => $tablename,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,buildrecords_fromrows($rowblock,$load_recursive),$row_offset);
            },
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_all_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
    );
}

sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,$tablename,$insert_ignore);

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
                $db->columnidentifier('country_code') . ' = ?' .
                ' AND ' . $db->columnidentifier('area_code') . ' = ?' .
                ' AND ' . $db->columnidentifier('dial_number') . ' = ?';
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
                   $tablename,
                   $expected_fieldnames,
                   $indexes);

}

sub subscribernumber {
    my $self = shift;
    return $self->{dial_number}; #$self->{country_code} . $self->{dial_number};
}

sub split_subscribernumber {
    my ($subscribernumber) = @_;
    my $country_code = substr($subscribernumber,0,3);
    my $area_code = 'None';
    my $dial_number = $subscribernumber; #substr($subscribernumber,3);
    return ($country_code,$area_code,$dial_number);
}

1;
