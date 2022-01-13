package NGCP::BulkProcessor::Dao::Trunk::accounting::events;
use strict;

#use Tie::IxHash;
## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_accounting_db
    destroy_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo

    copy_row

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    update_row
    insert_row

    process_subscribers
    process_events
    findby_subscriberid
);

my $tablename = 'events';
my $get_db = \&get_accounting_db;

my $expected_fieldnames = [
  'id',
  'type',
  'subscriber_id',
  'reseller_id',
  'old_status',
  'new_status',
  'timestamp',
  'export_status',
  'exported_at',
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

sub findby_subscriberid {

    my ($xa_db,$subscriber_id,$joins,$conditions,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my @conditions = @{$conditions // []};
    push(@conditions,{ $table . '.subscriber_id' => { '=' => '?' } });
    my $stmt = 'SELECT ' . join(',', map { $table . '.' . $db->columnidentifier($_); } @$expected_fieldnames) . ' ' .
        _get_export_stmt($db,$joins,\@conditions) .
        ' ORDER BY ' . $table . '.id ASC';
    my @params = ($subscriber_id);
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub process_subscribers {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $blocksize,
        $joins,
        $conditions,
        #$sort,
        $limit) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            blocksize
            joins
            conditions
            limit
        /};
    #sort

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);
    
    my $select_stmt;
    my $count_stmt;
    my $select_format = 'SELECT ' . $table . '.' . $db->columnidentifier('subscriber_id') . ' %s GROUP BY ' . $table . '.' . $db->columnidentifier('subscriber_id');
    my $count_format = 'SELECT COUNT(1) FROM (%s) AS __cnt';

    $select_stmt = sprintf($select_format,_get_export_stmt_part($db,$joins,$conditions));
    $count_stmt = sprintf($count_format,$db->paginate_sort_query('SELECT 1 ' . _get_export_stmt_part($db,$joins,$conditions),0,$limit,undef));
    
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
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
        blocksize                   => $blocksize,
        select                      => $select_stmt,
        selectcount                 => $count_stmt,
    );
}

sub process_events {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $blocksize,
        $joins,
        $conditions,
        $load_recursive,
        $limit) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            blocksize
            joins
            conditions
            load_recursive
            limit
        /};
    #sort

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);
    
    my $select_stmt;
    my $count_stmt;
    my $select_format = 'SELECT * %s';
    my $count_format = 'SELECT COUNT(1) FROM (%s) AS __cnt';

    $select_stmt = $db->paginate_sort_query(sprintf($select_format,_get_export_stmt_part($db,$joins,$conditions)),undef,undef,[
            {   numeric     => 1,
                dir         => 1, #-1,
                memberchain => [ 'id' ],
            }
        ]);
    $count_stmt = sprintf($count_format,$db->paginate_sort_query('SELECT 1 ' . _get_export_stmt_part($db,$joins,$conditions),0,$limit,undef));
    
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
        tableprocessing_threads     => $numofthreads,
        blocksize                   => $blocksize,
        select                      => $select_stmt,
        selectcount                 => $count_stmt,
    );
}

sub _get_export_stmt {

    my ($db,$joins,$conditions) = @_;
    return _get_export_stmt_part($db,undef,$joins,$conditions);
    
}

sub _get_export_stmt_part {

    my ($db,$static_context,$joins,$conditions) = @_;

    my $table = $db->tableidentifier($tablename);

    my $stmt = "FROM " . $table;
    my @intjoins = ();
    if (defined $joins and (scalar @$joins) > 0) {
        foreach my $f (@$joins) {
            my ($table, $keys) = %{ $f };
            my ($foreign_key, $own_key) = %{ $keys };
            push @intjoins, "LEFT JOIN $table ON $foreign_key = $own_key";
        }
    }

    my @conds = ();
    $stmt .= " " . join(" ", @intjoins) if (scalar @intjoins) > 0;
    if (defined $conditions and (scalar @$conditions) > 0) {
        foreach my $f (@$conditions) {
            my ($field, $match) = %{ $f };
            my ($op, $val) = %{ $match };
            push @conds, "$field $op $val";
        }
    }
    $stmt .= " WHERE " . join(" AND ", @conds) if (scalar @conds) > 0;
    return $stmt;

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
