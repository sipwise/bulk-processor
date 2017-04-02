package NGCP::BulkProcessor::Dao::Trunk::kamailio::acc_trash;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    process_table
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    process_records
    delete_callids
    findby_callids
    count
    count_callids
);

my $tablename = 'acc_trash';
my $get_db = \&get_kamailio_db;

my $expected_fieldnames = [
"id",
"method",
"from_tag",
"to_tag",
"callid",
"sip_code",
"sip_reason",
"time",
"time_hires",
"src_leg",
"dst_leg",
"dst_user",
"dst_ouser",
"dst_domain",
"src_user",
"src_domain",
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


sub delete_callids {

    my ($xa_db,$callids) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('callid') . ' IN (' . substr(',?' x scalar @$callids,1) . ')';
    my @params = @$callids;

    my $count;
    if ($count = $xa_db->db_do($stmt,@params)) {
        rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
        return 0;
    }

}

sub count {

    my ($xa_db) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;

    return $db->db_get_value($stmt);

}

sub count_callids {

    my ($xa_db) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(DISTINCT ' . $db->columnidentifier('callid') . ') FROM ' . $table;

    return $db->db_get_value($stmt);

}

sub findby_callids {

    my ($xa_db,$callids,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('callid') . ' IN (' . substr(',?' x scalar @$callids,1) . ')';
    my @params = @$callids;
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}


sub process_records {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $blocksize,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            blocksize
            numofthreads
            load_recursive
        /};

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return process_table(
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,buildrecords_fromrows($rowblock,$load_recursive),$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        blocksize                   => $blocksize,
        tableprocessing_threads     => $numofthreads,
        'select'                    => 'SELECT * FROM ' . $table . ' GROUP BY ' . $db->columnidentifier('callid'),
        'selectcount'               => 'SELECT COUNT(DISTINCT ' . $db->columnidentifier('callid') . ') FROM ' . $table,
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
