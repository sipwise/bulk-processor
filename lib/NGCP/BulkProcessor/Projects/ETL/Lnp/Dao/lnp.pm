package NGCP::BulkProcessor::Projects::ETL::Lnp::Dao::lnp;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::ETL::Lnp::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
    process_table
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

    @fieldnames
    has_rows

    update_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta
    
    find_carriers_by_delta

    process_records
);

my $tablename = 'lnp';
my $get_db = \&get_sqlite_db;

our @fieldnames = (
    'carrier_name',
    'carrier_prefix',
    'number',
    'routing_number',
    'start',
    'end',
    'authoritative',
    'skip_rewrite',
    'type',
    #calculated fields at the end!
    #'rownum',
    #'filenum',
    #'filename',
);

my $expected_fieldnames = [
    @fieldnames,
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'number' ];
my $indexes = {
    #$tablename . '_number' => [ 'number(32)' ],
    #$tablename . '_rownum' => [ 'rownum(11)' ],
    $tablename . '_delta' => [ 'delta(7)' ],
    $tablename . '_carrier_delta' => [ 'carrier_name(255)', 'carrier_prefix(32)', 'authoritative(1)', 'skip_rewrite(1)', 'delta(7)' ],
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

sub find_carriers_by_delta {

    my ($deltas,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = '';
    my @params = ();
    if (defined $deltas and 'HASH' eq ref $deltas) {
        foreach my $in (keys %$deltas) {
            my @values = (defined $deltas->{$in} and 'ARRAY' eq ref $deltas->{$in} ? @{$deltas->{$in}} : ($deltas->{$in}));
            $stmt .= ' AND ' if length($stmt);
            $stmt .= $db->columnidentifier('delta') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $deltas and length($deltas) > 0) {
        $stmt = $db->columnidentifier('delta') . ' = ?';
        push(@params,$deltas);
    }
    $stmt = ' WHERE ' . $stmt if length($stmt);
    
    $stmt = 'SELECT * FROM ' . $table . $stmt . ' GROUP BY '
      . $db->columnidentifier('carrier_name')
      . ', ' . $db->columnidentifier('carrier_prefix')
      . ', ' . $db->columnidentifier('authoritative')
      . ', ' . $db->columnidentifier('skip_rewrite');

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub update_delta {

    my ($number,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $number) {
        $stmt .= ' WHERE ' . $db->columnidentifier('number') . ' = ?';
        push(@params,$number);
    }

    return $db->db_do($stmt,@params);

}

sub countby_delta {

    my ($deltas) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = '';
    my @params = ();
    if (defined $deltas and 'HASH' eq ref $deltas) {
        foreach my $in (keys %$deltas) {
            my @values = (defined $deltas->{$in} and 'ARRAY' eq ref $deltas->{$in} ? @{$deltas->{$in}} : ($deltas->{$in}));
            $stmt .= ' AND ' if length($stmt);
            $stmt .= $db->columnidentifier('delta') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $deltas and length($deltas) > 0) {
        $stmt = $db->columnidentifier('delta') . ' = ?';
        push(@params,$deltas);
    }
    $stmt = ' WHERE ' . $stmt if length($stmt);
    
    $stmt = 'SELECT COUNT(*) FROM ' . $table . $stmt;

    return $db->db_get_value($stmt,@params);

}

sub has_rows {

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(1) FROM (SELECT 1 FROM ' . $table . ' LIMIT 1) AS q';
    
    return $db->db_get_value($stmt);
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

sub process_records {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $deltas) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            deltas
        /};

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my @terms = ();
    if (defined $deltas and 'HASH' eq ref $deltas) {
        foreach my $in (keys %$deltas) {
            my @values = (defined $deltas->{$in} and 'ARRAY' eq ref $deltas->{$in} ? @{$deltas->{$in}} : ($deltas->{$in}));
            push(@terms,$db->columnidentifier('delta') . ' ' . $in . ' ("' . join('","',@values) . '")');
        }
    } elsif (defined $deltas and length($deltas) > 0) {
        push(@terms,$db->columnidentifier('delta') . ' = "' . $deltas . '"');
    }
    
    return process_table(
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,$rowblock,$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_all_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        ((scalar @terms) ? ('select' => 'SELECT * FROM ' . $table . ' WHERE ' . join (' AND ',@terms)) : ()),
        ((scalar @terms) ? ('selectcount' => 'SELECT COUNT(1) FROM ' . $table . ' WHERE ' . join (' AND ',@terms)) : ()),
    );
}

sub carrier_hash {
    my $self = shift;
    return ($self->{carrier_name} // '') . '-' . ($self->{carrier_prefix} // '')
      . '-' . ($self->{authoritative} // '') . '-' . ($self->{skip_rewrite} // '');
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
                $db->columnidentifier('number') . ' = ?';
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
