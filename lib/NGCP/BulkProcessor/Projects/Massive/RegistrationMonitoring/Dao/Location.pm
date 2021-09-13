package NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::Dao::Location;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Massive::RegistrationMonitoring::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt
    transfer_table
);
#process_table
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    getupsertstatement
   
    findby_domainusername
    findby_ruid

    update_delta
    findby_delta
    countby_delta
    copy_table

    $deleted_delta
    $updated_delta
    $added_delta
    
    @fieldnames

);

my $tablename = 'location';
my $get_db = \&get_sqlite_db;

our @fieldnames = (
    'instance',
    'domain',
    'cseq',
    'partition',
    'ruid',
    'connection_id',
    'username',
    'keepalive',
    'path',
    'reg_id',
    'contact',
    'flags',
    'received',
    'callid',
    'socket',
    'cflags',
    'expires',
    'methods',
    'user_agent',
    'q',
    'last_modified',
    'server_id',
);

my $expected_fieldnames = [
    @fieldnames,
    'delta',
];

# table creation:
my $primarykey_fieldnames = [ 'ruid' ];
my $indexes = {
    #$tablename . '_number' => [ 'number(32)' ],
    #$tablename . '_rownum' => [ 'rownum(11)' ],
    $tablename . '_domain_username' => [ 'domain', 'username' ],
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

sub findby_ruid {

    my ($ruid,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return undef unless defined $ruid;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' . $table .
        ' WHERE ' . $db->columnidentifier('ruid') . ' = ?'
    , $ruid);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub update_delta {

    my ($ruid,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $ruid) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('ruid') . ' = ?';
        push(@params, $ruid);
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

#sub process_records {
#
#    my %params = @_;
#    my ($process_code,
#        $static_context,
#        $init_process_context_code,
#        $uninit_process_context_code,
#        $multithreading,
#        $numofthreads) = @params{qw/
#            process_code
#            static_context
#            init_process_context_code
#            uninit_process_context_code
#            multithreading
#            numofthreads
#        /};
#
#    check_table();
#    my $db = &$get_db();
#    my $table = $db->tableidentifier($tablename);
#
#    my @cols = map { $db->columnidentifier($_); } qw/domain sip_username/;
#
#    return process_table(
#        get_db                      => $get_db,
#        class                       => __PACKAGE__,
#        process_code                => sub {
#                my ($context,$rowblock,$row_offset) = @_;
#                return &$process_code($context,$rowblock,$row_offset);
#            },
#        static_context              => $static_context,
#        init_process_context_code   => $init_process_context_code,
#        uninit_process_context_code => $uninit_process_context_code,
#        destroy_reader_dbs_code     => \&destroy_all_dbs,
#        multithreading              => $multithreading,
#        tableprocessing_threads     => $numofthreads,
#        'select'                    => 'SELECT ' . join(',',@cols) . ' FROM ' . $table . ' GROUP BY ' . join(',',@cols),
#        'selectcount'              => 'SELECT COUNT(DISTINCT(' . join(',',@cols) . ')) FROM ' . $table,
#    );
#}

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
      join(', ', map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @$expected_fieldnames) . ')';
    my @values = ();
    foreach my $fieldname (@$expected_fieldnames) {
        if ('delta' eq $fieldname) {
            my $stmt = 'SELECT \'' . $updated_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('ruid') . ' = ?';
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
