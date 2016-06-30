# record base object

package SqlRecord;
use strict;

## no critic

#use Thread qw(async yield);
#use Thread::Queue;
use threads qw(yield);
use threads::shared;
use Thread::Queue;
#use Thread::Semaphore;
#use POSIX qw(ceil);

use Time::HiRes qw(sleep);

use Logging qw(
    getlogger
    fieldnamesaquired
    primarykeycolsaquired
    tableinfoscleared

    tablefixed

    tabletransferstarted
    tableprocessingstarted

    rowtransferstarted
    rowtransferred
    rowskipped
    rowinserted
    rowupdated
    rowsdeleted
    totalrowsdeleted
    rowinsertskipped
    rowupdateskipped
    tabletransferdone
    tableprocessingdone
    rowtransferdone

    fetching_rows
    writing_rows
    processing_rows

    tablethreadingdebug
);

use LogError qw(
    fieldnamesdiffer
    transferzerorowcount
    processzerorowcount
    deleterowserror
    tabletransferfailed
    tableprocessingfailed
);
#use LogWarn qw(calendarwarn);

use Table qw(get_rowhash);
use Array qw(setcontains contains);
use Utils qw(round threadid);
#use SQLiteDB qw(sqlitetablename);
#use ConnectorPool qw(destroy_dbs_thread);

#use LoadConfig;
use Globals qw(
$enablemultithreading
$cpucount
$cells_transfer_memory_limit
$defer_indexes);

#use Terminate qw(setsigkill);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    transfer_table
    copy_row
    process_table
    cleartableinfo
    checktableinfo
    registertableinfo
    create_targettable
    transfer_record
    transfer_records
    insert_record
    update_record
    delete_records
);

my $table_expected_fieldnames = {};
my $table_fieldnames_cached = {};
my $table_primarykeys = {};
my $table_target_indexes = {};

#my $logger = getlogger(__PACKAGE__);

my $tabletransfer_threadqueuelength = 5; #100; #30; #5; # ... >= 1
my $minblocksize = 100;
my $maxblocksize = 100000;
my $minnumberofchunks = 10;

my $tableprocessing_threadqueuelength = 10;
my $tableprocessing_threads = $cpucount; #3;

my $reader_connection_name = 'reader';
#my $writer_connection_name = 'writer';

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my ($get_db,$tablename,$expected_fieldnames,$target_indexes) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    checktableinfo($db,$tablename,$expected_fieldnames,$target_indexes);


    if (defined $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename}) { # and ref $table_fieldnames_cached->{$connectidentifier}->{$tablename} eq 'ARRAY') {
        # if there are fieldnames defined, we make a member variable for each and set it to undef
        foreach my $fieldname (@{$table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename}}) {
            $self->{$fieldname} = undef;
        }
    }

    return $self;

}

sub gethash {
    my $self = shift;
    my @fieldvalues = ();
    foreach my $field (sort keys %$self) { #http://www.perlmonks.org/?node_id=997682
        my $value = $self->{$field};
        if (ref $value eq '') {
            push(@fieldvalues,$value);
        }
    }
    return get_rowhash(\@fieldvalues);
}

sub cleartableinfo {

    my $get_db = shift;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;
    my $tid = threadid();

    my $connectidentifier = $db->connectidentifier();

    my $found = 0;

    if (exists $table_expected_fieldnames->{$tid}) {
        if (exists $table_expected_fieldnames->{$tid}->{$connectidentifier}) {
            delete $table_expected_fieldnames->{$tid}->{$connectidentifier};
            $found = 1;
        }
    }
    if (exists $table_fieldnames_cached->{$tid}){
        if (exists $table_fieldnames_cached->{$tid}->{$connectidentifier}) {
            delete $table_fieldnames_cached->{$tid}->{$connectidentifier};
            $found = 1;
        }
    }
    if (exists $table_primarykeys->{$tid}) {
        if (exists $table_primarykeys->{$tid}->{$connectidentifier}) {
            delete $table_primarykeys->{$tid}->{$connectidentifier};
            $found = 1;
        }
    }
    if (exists $table_target_indexes->{$tid}) {
        if (exists $table_target_indexes->{$tid}->{$connectidentifier}) {
            delete $table_target_indexes->{$tid}->{$connectidentifier};
            $found = 1;
        }
    }

    if ((scalar keys %{$table_expected_fieldnames->{$tid}}) == 0) {
        delete $table_expected_fieldnames->{$tid};
        $found = 1;
    }
    if ((scalar keys %{$table_fieldnames_cached->{$tid}}) == 0) {
        delete $table_fieldnames_cached->{$tid};
        $found = 1;
    }
    if ((scalar keys %{$table_primarykeys->{$tid}}) == 0) {
        delete $table_primarykeys->{$tid};
        $found = 1;
    }
    if ((scalar keys %{$table_target_indexes->{$tid}}) == 0) {
        delete $table_target_indexes->{$tid};
        $found = 1;
    }

    if ($found) {
        tableinfoscleared($db,getlogger(__PACKAGE__));
    }

}

sub registertableinfo {

    my ($get_db,$tablename,$fieldnames,$indexes,$keycols) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    if (not exists $table_expected_fieldnames->{$tid}) {
        $table_expected_fieldnames->{$tid} = {};
    }
    if (not exists $table_expected_fieldnames->{$tid}->{$connectidentifier}) {
        # create an empty category for the connection if none exists yet:
        $table_expected_fieldnames->{$tid}->{$connectidentifier} = {};
    }
    # we prefer to always update the expected fieldnames (that come from a derived class)
    $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename} = $fieldnames;

    if (not exists $table_fieldnames_cached->{$tid}) {
        $table_fieldnames_cached->{$tid} = {};
    }
    if (not exists $table_fieldnames_cached->{$tid}->{$connectidentifier}) {
        # create an empty fieldname cache for the connection if none exists yet:
        $table_fieldnames_cached->{$tid}->{$connectidentifier} = {};
    }

    if (not exists $table_primarykeys->{$tid}) {
        $table_primarykeys->{$tid} = {};
    }
    if (not exists $table_primarykeys->{$tid}->{$connectidentifier}) {
        # create an empty primary key column name cache for the connection if none exists yet:
        $table_primarykeys->{$tid}->{$connectidentifier} = {};
    }
    $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename} = $keycols;

    if (not exists $table_target_indexes->{$tid}) {
        $table_target_indexes->{$tid} = {};
    }
    if (not exists $table_target_indexes->{$tid}->{$connectidentifier}) {
        # create an empty index set list for target tables for the connection if none exists yet:
        $table_target_indexes->{$tid}->{$connectidentifier} = {};
    }
    # we prefer to always update the target table indexes (that come from a derived class)
    $table_target_indexes->{$tid}->{$connectidentifier}->{$tablename} = $indexes;

}

sub checktableinfo {

    my ($get_db,$tablename,$expected_fieldnames,$target_indexes) = @_;

    my $success = 1;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    if (not exists $table_expected_fieldnames->{$tid}) {
        #$table_expected_fieldnames->{$tid} = shared_clone({});
        $table_expected_fieldnames->{$tid} = {};
    }
    if (not exists $table_expected_fieldnames->{$tid}->{$connectidentifier}) {
        # create an empty category for the connection if none exists yet:
        #$table_expected_fieldnames->{$tid}->{$connectidentifier} = shared_clone({});
        $table_expected_fieldnames->{$tid}->{$connectidentifier} = {};
    }
    # we prefer to always update the expected fieldnames (that come from a derived class)
    #$table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename} = shared_clone($expected_fieldnames);
    $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename} = $expected_fieldnames;

    if (not exists $table_fieldnames_cached->{$tid}) {
        #$table_fieldnames_cached->{$tid} = shared_clone({});
        $table_fieldnames_cached->{$tid} = {};
    }
    if (not exists $table_fieldnames_cached->{$tid}->{$connectidentifier}) {
        # create an empty fieldname cache for the connection if none exists yet:
        #$table_fieldnames_cached->{$tid}->{$connectidentifier} = shared_clone({});
        $table_fieldnames_cached->{$tid}->{$connectidentifier} = {};
    }

    if (not exists $table_fieldnames_cached->{$tid}->{$connectidentifier}->{$tablename}) {
        # query the database for fieldnames of the table if we don't have a cache entry yet:
        #$table_fieldnames_cached->{$tid}->{$connectidentifier}->{$tablename} = shared_clone($db->getfieldnames($tablename));
        $table_fieldnames_cached->{$tid}->{$connectidentifier}->{$tablename} = $db->getfieldnames($tablename);
        #my $fieldnames = $db->getfieldnames($tablename);
        if (!defined $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename} or setcontains($table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename},$table_fieldnames_cached->{$tid}->{$connectidentifier}->{$tablename},1)) { #fieldnames are case insensitive in general
            # if not expected fieldnames are given or queried fieldnames match, we log this:
            #$table_fieldnames_cached->{$connectidentifier}->{$tablename} = $table_expected_fieldnames->{$connectidentifier}->{$tablename};
            fieldnamesaquired($db,$tablename,getlogger(__PACKAGE__));
        } else {
            # otherwise we log a failure (exit? - see Logging Module)
            #$table_fieldnames_cached->{$connectidentifier}->{$tablename} = {}; #$fieldnames;
            fieldnamesdiffer($db,$tablename,$table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename},$table_fieldnames_cached->{$tid}->{$connectidentifier}->{$tablename},getlogger(__PACKAGE__));
            $success = 0;
        }
    }

    if (not exists $table_primarykeys->{$tid}) {
        #$table_primarykeys->{$tid} = shared_clone({});
        $table_primarykeys->{$tid} = {};
    }
    if (not exists $table_primarykeys->{$tid}->{$connectidentifier}) {
        # create an empty primary key column name cache for the connection if none exists yet:
        #$table_primarykeys->{$tid}->{$connectidentifier} = shared_clone({});
        $table_primarykeys->{$tid}->{$connectidentifier} = {};
    }
    if (not exists $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename}) {
        # query the database for primary keys of the table if we don't have them cached yet:
        #$table_primarykeys->{$tid}->{$connectidentifier}->{$tablename} = shared_clone($db->getprimarykeycols($tablename));
        $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename} = $db->getprimarykeycols($tablename);
        primarykeycolsaquired($db,$tablename,$table_primarykeys->{$tid}->{$connectidentifier}->{$tablename},getlogger(__PACKAGE__));
    }

    if (not exists $table_target_indexes->{$tid}) {
        #$table_target_indexes->{$tid} = shared_clone({});
        $table_target_indexes->{$tid} = {};
    }
    if (not exists $table_target_indexes->{$tid}->{$connectidentifier}) {
        # create an empty index set list for target tables for the connection if none exists yet:
        #$table_target_indexes->{$tid}->{$connectidentifier} = shared_clone({});
        $table_target_indexes->{$tid}->{$connectidentifier} = {};
    }
    # we prefer to always update the target table indexes (that come from a derived class)
    #$table_target_indexes->{$tid}->{$connectidentifier}->{$tablename} = shared_clone($target_indexes);
    $table_target_indexes->{$tid}->{$connectidentifier}->{$tablename} = $target_indexes;

    return $success;

}

sub create_targettable {

    my ($get_db,$tablename,$get_target_db,$targettablename,$truncate,$defer_indexes,$texttable_engine) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;

    #my $targettablename = _gettargettablename($db,$tablename,$target_db);
    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    if ($truncate and $defer_indexes) {
       $target_db->drop_table($targettablename);
    }

    my $result = $target_db->create_texttable($targettablename,
                                 $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename},
                                 $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename},
                                 $table_target_indexes->{$tid}->{$connectidentifier}->{$tablename},
                                 # 'ifnotexists' is always true
                                 $truncate,
                                 $defer_indexes,
                                 $texttable_engine);

    checktableinfo($target_db,$targettablename,$table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename},$defer_indexes ? undef : $table_target_indexes->{$tid}->{$connectidentifier}->{$tablename});
    return $result;

}

sub delete_records {

    my ($get_db,$tablename,$keyfields,$equal,$vals_table) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};
    my $primarykeys = $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename};

    if (defined $expected_fieldnames and
        (defined $keyfields and
        ref $keyfields eq 'ARRAY') and
        (defined $vals_table and
        ref $vals_table eq 'Table')) {

        my @fields = @$keyfields;
        my $field_cnt = scalar @fields;

        my $total_rowcount = 0;

        my $initial_rowcount = $db->db_get_value('SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename));

        if ($field_cnt > 0) {
            my $where_clause;
            if ($equal) {
                $where_clause = ' WHERE ' . join(' = ? AND ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fields) . ' = ?';

                my $count_stmt = 'SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename) . $where_clause;
                my $delete_stmt = 'DELETE FROM ' . $db->tableidentifier($tablename) . $where_clause;

                for (my $i = 0; $i < $vals_table->rowcount(); $i++) {
                    my @vals = $vals_table->getrow($i);
                    my $new_initial_rowcount = $db->db_get_value('SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename));
                    my $rowcount = $db->db_get_value($count_stmt,@vals);
                    $db->db_do($delete_stmt,@vals);
                    rowsdeleted($db,$tablename,$rowcount,$new_initial_rowcount,getlogger(__PACKAGE__));
                    $total_rowcount += $rowcount;
                }

            } elsif ($field_cnt == 1)  {
                my @ne_vals = $vals_table->getcol(0);
                $where_clause = ' WHERE ' . $db->columnidentifier($fields[0]) . ' NOT IN (' . substr(',?' x scalar @ne_vals,1) . ')';
                my $count_stmt = 'SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename) . $where_clause;
                my $delete_stmt = 'DELETE FROM ' . $db->tableidentifier($tablename) . $where_clause;
                my $rowcount = $db->db_get_value($count_stmt,@ne_vals);
                $db->db_do($delete_stmt,@ne_vals);
                rowsdeleted($db,$tablename,$rowcount,$initial_rowcount,getlogger(__PACKAGE__));
                $total_rowcount += $rowcount;
            } else {

                deleterowserror($db,$tablename,'deletings rows by complementary identifier values works with a single identifier column only',getlogger(__PACKAGE__));
                return;

            }
        } else {
            my $delete_stmt = 'DELETE FROM ' . $db->tableidentifier($tablename);
            my $count_stmt = 'SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename);
            my $rowcount = $db->db_get_value($count_stmt);
            $db->db_do($delete_stmt);
            rowsdeleted($db,$tablename,$rowcount,$initial_rowcount,getlogger(__PACKAGE__));
            $total_rowcount += $rowcount;
        }

        $db->vacuum($tablename);

        #if ($total_rowcount > 0) {
            totalrowsdeleted($db,$tablename,$total_rowcount,$initial_rowcount,getlogger(__PACKAGE__));
        #}

        return $total_rowcount;

    }

}

sub insert_record {

    my ($get_db,$tablename,$allowdupes,$row) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    #my $targettablename = _gettargettablename($db,$tablename,$target_db); #$target_db->getsafetablename($db->tableidentifier($tablename));
    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};
    my $primarykeys = $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename};

    if (defined $expected_fieldnames and defined $row) {

        my @fieldnames = @$expected_fieldnames;
        my @fields = ();
        my @vals = ();

        foreach my $fieldname (@fieldnames) {
            if (exists $row->{$fieldname}) {
                push @fields,$fieldname;
                push @vals,$row->{$fieldname};
            }
        }

        my @pk_fieldnames;
        my @pk_fields = ();
        my @pk_vals = ();

        if (not $allowdupes) {
            if (defined $primarykeys) {
                @pk_fieldnames = @$primarykeys;
                if (scalar @pk_fieldnames > 0) {
                    foreach my $fieldname (@pk_fieldnames) {
                        if (exists $row->{$fieldname}) {
                            push @pk_fields,$fieldname;
                            push @pk_vals,$row->{$fieldname};
                        #} else {
                        #    'insert error: pk field not foun din row';
                        #    push @pk_vals,undef;
                        }
                    }
                } else {
                    @pk_fields = @fields;
                    @pk_vals = @vals;
                }
            } else {
                @pk_fields = @fields;
                @pk_vals = @vals;
            }
        }

        if ($allowdupes or $db->db_get_value('SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename) . ' WHERE ' . join(' = ? AND ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @pk_fields) . ' = ?',@pk_vals) == 0) {
            $db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fields) . ') VALUES (' . substr(',?' x scalar @fields,1) . ')',@vals);
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return 1;
        } else {
            rowinsertskipped($db,$tablename,getlogger(__PACKAGE__));
            return 0;
        }

    }

}

sub update_record {

    my ($get_db,$tablename,$row) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;

    #my $targettablename = _gettargettablename($db,$tablename,$target_db); #$target_db->getsafetablename($db->tableidentifier($tablename));
    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};
    my $primarykeys = $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename};

    if (defined $expected_fieldnames and defined $row) {

        my @fieldnames = @$expected_fieldnames;
        my @fields = ();
        my @vals = ();

        foreach my $fieldname (@fieldnames) {
            if (exists $row->{$fieldname}) {
                push @fields,$fieldname;
                push @vals,$row->{$fieldname};
            }
        }

        my @pk_fieldnames;
        my @pk_fields = ();
        my @pk_vals = ();

        if (defined $primarykeys) {
            @pk_fieldnames = @$primarykeys;
            if (scalar @pk_fieldnames > 0) {
                foreach my $fieldname (@pk_fieldnames) {
                    if (exists $row->{$fieldname}) {
                        push @pk_fields,$fieldname;
                        push @pk_vals,$row->{$fieldname};
                    #} else {
                    #    'insert error: pk field not foun din row';
                    #    push @pk_vals,undef;
                    }
                }
            } else {
                @pk_fields = @fields;
                @pk_vals = @vals;
            }
        } else {
            @pk_fields = @fields;
            @pk_vals = @vals;
        }

        my $selectpk_fieldnames = join(' = ? AND ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @pk_fields);

        if ($db->db_get_value('SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename) . ' WHERE ' . $selectpk_fieldnames . ' = ?',@pk_vals) == 1) {
            $db->db_do('UPDATE ' . $db->tableidentifier($tablename) . ' SET ' . join(' = ?, ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fields) . ' = ? WHERE ' . $selectpk_fieldnames . ' = ?',@vals,@pk_vals);
            rowupdated($db,$tablename,getlogger(__PACKAGE__));
            return 1;
        } else {
            rowupdateskipped($db,$tablename,getlogger(__PACKAGE__));
            return 0;
        }

    }

}

sub transfer_record {

    #my $self = shift
    my ($get_db,$tablename,$get_target_db,$targettablename,$allowdupes,$row) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;

    #my $targettablename = _gettargettablename($db,$tablename,$target_db); #$target_db->getsafetablename($db->tableidentifier($tablename));
    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};
    my $primarykeys = $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename};

    if (defined $expected_fieldnames and defined $row) {

        my @fieldnames = @$expected_fieldnames;
        my @vals = ();

        foreach my $fieldname (@fieldnames) {
            push @vals,$row->{$fieldname};
        }

        my @pk_fieldnames;
        my @pk_vals = ();

        if (not $allowdupes) {
            if (defined $primarykeys) {
                @pk_fieldnames = @$primarykeys;
                if (scalar @pk_fieldnames > 0) {
                    foreach my $fieldname (@pk_fieldnames) {
                        push @pk_vals,$row->{$fieldname};
                    }
                } else {
                    @pk_fieldnames = @fieldnames;
                    @pk_vals = @vals;
                }
            } else {
                @pk_fieldnames = @fieldnames;
                @pk_vals = @vals;
            }
        }

        if ($allowdupes or $target_db->db_get_value('SELECT COUNT(*) FROM ' . $target_db->tableidentifier($targettablename) . ' WHERE ' . join(' = ? AND ',map { local $_ = $_; $_ = $target_db->columnidentifier($_); $_; } @pk_fieldnames) . ' = ?',@pk_vals) == 0) {
            $target_db->db_do('INSERT INTO ' . $target_db->tableidentifier($targettablename) . ' (' . join(', ',map { local $_ = $_; $_ = $target_db->columnidentifier($_); $_; } @fieldnames) . ') VALUES (' . substr(',?' x scalar @fieldnames,1) . ')',@vals);
            rowtransferred($db,$tablename,$target_db,$targettablename,1,1,getlogger(__PACKAGE__));
            return 1;
        } else {
            rowskipped($db,$tablename,$target_db,$targettablename,1,1,getlogger(__PACKAGE__));
            return 0;
        }

    }

}

sub transfer_records {

    my ($get_db,$tablename,$get_target_db,$targettablename,$allowdupes,$rows) = @_;

    my $db = (ref $get_db eq 'CODE') ? &$get_db() : $get_db;
    my $target_db = (ref $get_target_db eq 'CODE') ? &$get_target_db() : $get_target_db;

    #my $targettablename = _gettargettablename($db,$tablename,$target_db); #$target_db->getsafetablename($db->tableidentifier($tablename));
    my $connectidentifier = $db->connectidentifier();
    my $tid = threadid();

    my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};
    my $primarykeys = $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename};

    if (defined $expected_fieldnames and defined $rows and ref $rows eq 'ARRAY') { # and defined $getrecords_code and ref $getrecords_code eq 'CODE') {
        #get_local_db();

        my $numofrows = scalar @$rows;
        rowtransferstarted($db,$tablename,$target_db,$targettablename,$numofrows,getlogger(__PACKAGE__));

        my @fieldnames = @$expected_fieldnames;

        my $setfieldnames = join(', ',map { local $_ = $_; $_ = $target_db->columnidentifier($_); $_; } @fieldnames);
        my $valueplaceholders = substr(',?' x scalar @fieldnames,1);

        my $rowstransferred = 0;

        if ($allowdupes) {

            my @rows_array = ();
            $target_db->db_do_begin('INSERT INTO ' . $target_db->tableidentifier($targettablename) . ' (' . $setfieldnames . ') VALUES (' . $valueplaceholders . ')');
            foreach my $row (@$rows) {
                my @vals = ();
                foreach my $fieldname (@fieldnames) {
                    push @vals,$row->{$fieldname};
                }
                push @rows_array,\@vals;
            }
            $target_db->db_do_rowblock(\@rows_array);
            $target_db->db_finish();
            $rowstransferred = scalar @rows_array;

        } else {

            my $i = 1;

            my @pk_fieldnames;

            if (defined $primarykeys) {
                @pk_fieldnames = @$primarykeys;
                if (scalar @pk_fieldnames == 0) {
                    @pk_fieldnames = @fieldnames;
                }
            } else {
                @pk_fieldnames = @fieldnames;
            }

            my $selectpk_fieldnames = join(' = ? AND ',map { local $_ = $_; $_ = $target_db->columnidentifier($_); $_; } @pk_fieldnames) . ' = ?';

            foreach my $row (@$rows) {

                my @vals = ();

                foreach my $fieldname (@fieldnames) {
                    push @vals,$row->{$fieldname};
                }

                my @pk_vals;

                foreach my $fieldname (@pk_fieldnames) {
                    push @pk_vals,$row->{$fieldname};
                }

                if ($target_db->db_get_value('SELECT COUNT(*) FROM ' . $target_db->tableidentifier($targettablename) . ' WHERE ' . $selectpk_fieldnames,@pk_vals) == 0) {
                    $target_db->db_do('INSERT INTO ' . $db->target_tableidentifier($targettablename) . ' (' . $setfieldnames . ') VALUES (' . $valueplaceholders . ')',@vals);
                    rowtransferred($db,$tablename,$target_db,$targettablename,$i,$numofrows,getlogger(__PACKAGE__));
                    $rowstransferred += 1;
                } else {
                    rowskipped($db,$tablename,$target_db,$targettablename,$i,$numofrows,getlogger(__PACKAGE__));
                }
                $i++;
            }
        }
        rowtransferdone($db,$tablename,$target_db,$targettablename,$numofrows,getlogger(__PACKAGE__));
        return $rowstransferred;
    }

}

sub transfer_table {

    my ($get_db,$tablename,$get_target_db,$targettablename,$truncate_targettable,$create_indexes,$texttable_engine,$fixtable_statements,$selectcount,$select,@values) = @_;

    if (ref $get_db eq 'CODE' and ref $get_target_db eq 'CODE') {

        my $db = &$get_db($reader_connection_name,1);
        my $target_db = &$get_target_db(); #$writer_connection_name);

        my $countstatement;
        if (defined $selectcount) {
            $countstatement = $selectcount;
        } else {
            $countstatement = 'SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename);
        }

        my $rowcount = $db->db_get_value($countstatement,@values);

        #my $targettablename = _gettargettablename($db,$tablename,$target_db); #$target_db->getsafetablename($db->tableidentifier($tablename));

        if ($rowcount > 0) {
            tabletransferstarted($db,$tablename,$target_db,$targettablename,$rowcount,getlogger(__PACKAGE__));
        } else {
            transferzerorowcount($db,$tablename,$target_db,$targettablename,$rowcount,getlogger(__PACKAGE__));
            return;
        }

        my $errorstate = $RUNNING; # 1;

        $create_indexes = ((defined $create_indexes) ? $create_indexes : $defer_indexes);

        if (create_targettable($db,$tablename,$target_db,$targettablename,$truncate_targettable,$create_indexes,$texttable_engine)) {

            my $connectidentifier = $db->connectidentifier();
            my $tid = threadid();
            my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};

            my @fieldnames = @$expected_fieldnames;

            #my $setfieldnames = join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fieldnames);
            my $valueplaceholders = substr(',?' x scalar @fieldnames,1);

            my $selectstatement;
            if (length($select) > 0) {
                $selectstatement = $select;
            } else {
                $selectstatement = 'SELECT ' . join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fieldnames) . ' FROM ' . $db->tableidentifier($tablename)
            }

        my $insertstatement = 'INSERT INTO ' . $target_db->tableidentifier($targettablename) . ' (' . join(', ',map { local $_ = $_; $_ = $target_db->columnidentifier($_); $_; } @fieldnames) . ') VALUES (' . $valueplaceholders . ')';

            my $blocksize;

            if ($enablemultithreading and $db->multithreading_supported() and $target_db->multithreading_supported() and $cpucount > 1) { # and $multithreaded) { # definitely no multithreading when CSVDB is involved

                $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,1,$tabletransfer_threadqueuelength);

                my $reader;
                my $writer;

                my %errorstates :shared = ();
                #$errorstates{$tid} = $errorstate;

                #my $readererrorstate :shared = 1;
                #my $writererrorstate :shared = 1;

                my $queue = Thread::Queue->new();

                tablethreadingdebug('shutting down db connections ...',getlogger(__PACKAGE__));

                $db->db_disconnect();
                #undef $db;
                $target_db->db_disconnect();
                #undef $target_db;
                my $default_connection = &$get_db(undef,0);
                my $default_connection_reconnect = $default_connection->is_connected();
                $default_connection->db_disconnect();

                tablethreadingdebug('starting reader thread',getlogger(__PACKAGE__));

                $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            #readererrorstate_ref => \$readererrorstate,
                                            #writererrorstate_ref => \$writererrorstate,
                                            threadqueuelength    => $tabletransfer_threadqueuelength,
                                            get_db               => $get_db,
                                            tablename            => $tablename,
                                            selectstatement      => $selectstatement,
                                            blocksize            => $blocksize,
                                            rowcount             => $rowcount,
                                            #logger               => $logger,
                                            values_ref           => \@values,
                                          });

                tablethreadingdebug('starting writer thread',getlogger(__PACKAGE__));

                $writer = threads->create(\&_writer,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            readertid              => $reader->tid(),
                                            #readererrorstate_ref => \$readererrorstate,
                                            #writererrorstate_ref => \$writererrorstate,
                                            get_target_db        => $get_target_db,
                                            targettablename      => $targettablename,
                                            insertstatement      => $insertstatement,
                                            blocksize            => $blocksize,
                                            rowcount             => $rowcount,
                                            #logger               => $logger,
                                          });

                $reader->join();
                tablethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
                $writer->join();
                tablethreadingdebug('writer thread joined',getlogger(__PACKAGE__));

                #$errorstate = $readererrorstate | $writererrorstate;
                $errorstate = _get_other_threads_state(\%errorstates,$tid);

                tablethreadingdebug('restoring db connections ...',getlogger(__PACKAGE__));

                #$db = &$get_db($reader_connection_name,1);
                $target_db = &$get_target_db(undef,1);
                if ($default_connection_reconnect) {
                    $default_connection = &$get_db(undef,1);
                }

            } else {

                $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,0,undef);

                #$db->db_disconnect();
                #undef $db;
                #$db = &$get_db($reader_connection_name);
                #$target_db->db_disconnect();
                #undef $target_db;
                #$target_db = &$get_target_db($writer_connection_name);

                eval {
                    $db->db_get_begin($selectstatement,$tablename,@values);

                    my $i = 0;
                    while (1) {
                        fetching_rows($db,$tablename,$i,$blocksize,$rowcount,getlogger(__PACKAGE__));
                        my $rowblock = $db->db_get_rowblock($blocksize);
                        my $realblocksize = scalar @$rowblock;
                        if ($realblocksize > 0) {
                            writing_rows($target_db,$targettablename,$i,$realblocksize,$rowcount,getlogger(__PACKAGE__));
                            $target_db->db_do_begin($insertstatement,$targettablename);
                            $target_db->db_do_rowblock($rowblock);
                            $target_db->db_finish();
                            $i += $realblocksize;

                            #foreach my $row (@$rowblock) {
                            #    undef $row;
                            #}
                            #undef $rowblock;

                            if ($realblocksize < $blocksize) {
                                last;
                            }
                        } else {
                            last;
                        }
                    }
                    $db->db_finish();

                };

                if ($@) {
                    $errorstate = $ERROR;
                } else {
                    $errorstate = $COMPLETED;
                }

                $db->db_disconnect();
                #undef $db;
                #$target_db->db_disconnect();
                #undef $target_db;

            }

            #$db = &$get_db($controller_name,1);
            #$target_db = &$get_target_db($controller_name,1);

            if ($errorstate == $COMPLETED and ref $fixtable_statements eq 'ARRAY' and (scalar @$fixtable_statements) > 0) {
                eval {
                    foreach my $fixtable_statement (@$fixtable_statements) {
                        if (ref $fixtable_statement eq '') {
                            $target_db->db_do($fixtable_statement);
                            tablefixed($target_db,$targettablename,$fixtable_statement,getlogger(__PACKAGE__));
                        } else {
                            $fixtable_statement = &$fixtable_statement($target_db->tableidentifier($targettablename));
                            $target_db->db_do($fixtable_statement);
                            tablefixed($target_db,$targettablename,$fixtable_statement,getlogger(__PACKAGE__));
                        }

                    }
                };
                if ($@) {
                    $errorstate = $ERROR;
                #} else {
                #    $errorstate = $COMPLETED;
                }
            }

            if ($errorstate == $COMPLETED and $create_indexes) {

                eval {
                    $target_db->create_primarykey($targettablename,
                        $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename},
                        $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename});

                    $target_db->create_indexes($targettablename,
                        $table_target_indexes->{$tid}->{$connectidentifier}->{$tablename},
                        $table_primarykeys->{$tid}->{$connectidentifier}->{$tablename});


                    delete $table_primarykeys->{$tid}->{$target_db->connectidentifier()}->{$targettablename};
                    checktableinfo($target_db,$targettablename,$table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename},$table_target_indexes->{$tid}->{$connectidentifier}->{$tablename});

                    $target_db->vacuum($targettablename);

                };

                if ($@) {
                    $errorstate = $ERROR;
                #} else {
                #    $errorstate = $COMPLETED;
                }
            }

        }

        if ($errorstate == $COMPLETED) {
            tabletransferdone($db,$tablename,$target_db,$targettablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
            #$target_db->db_disconnect();
            return 1;
        } else {
            tabletransferfailed($db,$tablename,$target_db,$targettablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
            #$target_db->db_disconnect();
        }

    }

    return 0;

}

sub process_table {

    my ($get_db,$tablename,$process_code,$multithreading,$selectcount,$select,@values) = @_;

    if (ref $get_db eq 'CODE') {

        my $db = &$get_db($reader_connection_name,1);

        my $countstatement;
        if (defined $selectcount) {
            $countstatement = $selectcount;
        } else {
            $countstatement = 'SELECT COUNT(*) FROM ' . $db->tableidentifier($tablename);
        }

        my $rowcount = $db->db_get_value($countstatement,@values);

        if ($rowcount > 0) {
            tableprocessingstarted($db,$tablename,$rowcount,getlogger(__PACKAGE__));
        } else {
            processzerorowcount($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            return;
        }

        my $errorstate = $RUNNING;

        my $connectidentifier = $db->connectidentifier();
        my $tid = threadid();
        my $expected_fieldnames = $table_expected_fieldnames->{$tid}->{$connectidentifier}->{$tablename};

        my @fieldnames = @$expected_fieldnames;

        #my $setfieldnames = join(', ',@fieldnames);
        #my $valueplaceholders = substr(',?' x scalar @fieldnames,1);

        my $selectstatement;
        if (length($select) > 0) {
            $selectstatement = $select;
        } else {
            $selectstatement = 'SELECT ' . join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @fieldnames) . ' FROM ' . $db->tableidentifier($tablename);
        }

        my $blocksize;

        if ($enablemultithreading and $multithreading and $db->multithreading_supported() and $cpucount > 1) { # and $multithreaded) { # definitely no multithreading when CSVDB is involved

            $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,1,$tableprocessing_threadqueuelength);

            my $reader;
            #my $processor;
            my %processors = ();

            my %errorstates :shared = ();
            #$errorstates{$tid} = $errorstate;

            #my $readererrorstate :shared = 1;
            #my $processorerrorstate :shared = 1;

            my $queue = Thread::Queue->new();

            tablethreadingdebug('shutting down db connections ...',getlogger(__PACKAGE__));

            $db->db_disconnect();
            #undef $db;
            my $default_connection = &$get_db(undef,0);
            my $default_connection_reconnect = $default_connection->is_connected();
            $default_connection->db_disconnect();

            tablethreadingdebug('starting reader thread',getlogger(__PACKAGE__));

            $reader = threads->create(\&_reader,
                                          { queue                => $queue,
                                            errorstates          => \%errorstates,
                                            #readererrorstate_ref => \$readererrorstate,
                                            #writererrorstate_ref => \$processorerrorstate,
                                            threadqueuelength    => $tableprocessing_threadqueuelength,
                                            get_db               => $get_db,
                                            tablename            => $tablename,
                                            selectstatement      => $selectstatement,
                                            blocksize            => $blocksize,
                                            rowcount             => $rowcount,
                                            #logger               => $logger,
                                            values_ref           => \@values,
                                          });

            for (my $i = 0; $i < $tableprocessing_threads; $i++) {
                tablethreadingdebug('starting processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads,getlogger(__PACKAGE__));
                my $processor = threads->create(\&_process,
                                              { queue                => $queue,
                                                errorstates          => \%errorstates,
                                                readertid              => $reader->tid(),
                                                #readererrorstate_ref => \$readererrorstate,
                                                #processorerrorstate_ref => \$processorerrorstate,
                                                process_code         => $process_code,
                                                blocksize            => $blocksize,
                                                rowcount             => $rowcount,
                                                #logger               => $logger,
                                              });
                if (!defined $processor) {
                    tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT started',getlogger(__PACKAGE__));
                }
                $processors{$processor->tid()} = $processor;
                #push (@processors,$processor);
            }

            #$reader->join();
            #tablethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            #for (my $i = 0; $i < $tableprocessing_threads; $i++) {
            #    my $processor = $processors[$i];
            #    if (defined $processor) {
            #        $processor->join();
            #        tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' joinded',getlogger(__PACKAGE__));
            #    } else {
            #        tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT joinded',getlogger(__PACKAGE__));
            #    }
            #}

            $reader->join();
            tablethreadingdebug('reader thread joined',getlogger(__PACKAGE__));
            #print 'threads running: ' . (scalar threads->list(threads::running));
            #while ((scalar threads->list(threads::running)) > 1 or (scalar threads->list(threads::joinable)) > 0) {
            while ((scalar keys %processors) > 0) {
                #for (my $i = 0; $i < $tableprocessing_threads; $i++) {
                foreach my $processor (values %processors) {
                    #my $processor = $processors[$i];
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        #tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' joinded',getlogger(__PACKAGE__));
                        tablethreadingdebug('processor thread tid ' . $processor->tid() . ' joined',getlogger(__PACKAGE__));
                    }
                    #} else {
                    #    tablethreadingdebug('processor thread ' . ($i + 1) . ' of ' . $tableprocessing_threads . ' NOT joinded',getlogger(__PACKAGE__));
                    #}
                }
                sleep($thread_sleep_secs);
            }

            #$errorstate = $readererrorstate | $processorerrorstate;
            $errorstate = (_get_other_threads_state(\%errorstates,$tid) & ~$RUNNING);

            tablethreadingdebug('restoring db connections ...',getlogger(__PACKAGE__));

            #$db = &$get_db($reader_connection_name,1);
            if ($default_connection_reconnect) {
                $default_connection = &$get_db(undef,1);
            }

        } else {

            $blocksize = _calc_blocksize($rowcount,scalar @fieldnames,0,undef);
            #$db->db_disconnect();
            #undef $db;
            #$db = &$get_db($reader_connection_name);

            my $rowblock_result = 1;
            eval {
                $db->db_get_begin($selectstatement,$tablename,@values);

                my $i = 0;
                while (1) {
                    fetching_rows($db,$tablename,$i,$blocksize,$rowcount,getlogger(__PACKAGE__));
                    my $rowblock = $db->db_get_rowblock($blocksize);
                    my $realblocksize = scalar @$rowblock;
                    if ($realblocksize > 0) {
                        processing_rows($tid,$i,$realblocksize,$rowcount,getlogger(__PACKAGE__));

                        $rowblock_result = &$process_code($rowblock,$i);

                        #$target_db->db_do_begin($insertstatement,$targettablename);
                        #$target_db->db_do_rowblock($rowblock);
                        #$target_db->db_finish();
                        $i += $realblocksize;

                        if ($realblocksize < $blocksize || not $rowblock_result) {
                             last;
                        }
                    } else {
                        last;
                    }
                }
                $db->db_finish();

            };

            if ($@) {
                $errorstate = $ERROR;
            } else {
                $errorstate = (not $rowblock_result) ? $ERROR : $COMPLETED;
            }

            $db->db_disconnect();
            #undef $db;

        }

        #$db = &$get_db($controller_name,1);

        if ($errorstate == $COMPLETED) {
            tableprocessingdone($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
            return 1;
        } else {
            tableprocessingfailed($db,$tablename,$rowcount,getlogger(__PACKAGE__));
            #$db->db_disconnect();
        }

    }

    return 0;

}

sub _calc_blocksize {

    my ($rowcount,$columncount,$multithreaded,$threadqueuelength) = @_;

    if ($rowcount > $minblocksize) {

        my $exp = int ( log ($rowcount) / log(10.0) );
        my $blocksize = int ( 10 ** $exp );
        my $cellcount_in_memory = $columncount * $blocksize;
        if ($multithreaded) {
            $cellcount_in_memory *= $threadqueuelength;
        }

        while ( $cellcount_in_memory > $cells_transfer_memory_limit or
                $rowcount / $blocksize < $minnumberofchunks) {
            $exp -= 1.0;
            $blocksize = int ( 10 ** $exp );
            $cellcount_in_memory = $columncount * $blocksize;
            if ($multithreaded) {
                $cellcount_in_memory *= $threadqueuelength;
            }
        }

        if ($blocksize < $minblocksize) {
            return $minblocksize;
        } elsif ($blocksize > $maxblocksize) {
            return $maxblocksize;
        } else {
            return $blocksize;
        }

    } else {

        return $minblocksize;

    }

}

sub _get_other_threads_state {
    my ($errorstates,$tid) = @_;
    my $result = 0;
    if (!defined $tid) {
        $tid = threadid();
    }
    if (defined $errorstates and ref $errorstates eq 'HASH') {
        lock $errorstates;
        foreach my $threadid (keys %$errorstates) {
            if ($threadid != $tid) {
                $result |= $errorstates->{$threadid};
            }
        }
    }
    return $result;
}

sub _get_stop_consumer_thread {
    my ($context,$tid) = @_;
    my $result = 1;
    my $other_threads_state;
    my $reader_state;
    my $queuesize;
    {
        my $errorstates = $context->{errorstates};
        lock $errorstates;
        $other_threads_state = _get_other_threads_state($errorstates,$tid);
        $reader_state = $errorstates->{$context->{readertid}};
    }
    $queuesize = $context->{queue}->pending();
    if (($other_threads_state & $ERROR) == 0 and ($queuesize > 0 or $reader_state == $RUNNING)) {
        $result = 0;
        #keep the consumer thread running if there is no defunct thread and queue is not empty or reader is still running
    }

    if ($result) {
        tablethreadingdebug('[' . $tid . '] consumer thread is shutting down (' .
                            (($other_threads_state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ', ' .
                            ($queuesize > 0 ? 'blocks pending' : 'no blocks pending') . ', ' .
                            ($reader_state == $RUNNING ? 'reader thread running' : 'reader thread not running') . ') ...'
        ,getlogger(__PACKAGE__));
    }

    return $result;

}

sub _reader {

    #my ($queue,$readererrorstate_ref,$writererrorstate_ref,$get_db,$tablename,$selectstatement,$blocksize,$rowcount,$logger,@values) = @_;
    my $context = shift;

    my $reader_db;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    tablethreadingdebug('[' . $tid . '] reader thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        $reader_db = &{$context->{get_db}}(); #$reader_connection_name);
        $reader_db->db_get_begin($context->{selectstatement},$context->{tablename},@{$context->{values_ref}});
        my $i = 0;
        tablethreadingdebug('[' . $tid . '] reader thread waiting for consumer threads',getlogger(__PACKAGE__));
        while ((_get_other_threads_state($context->{errorstates},$tid) & $RUNNING) == 0) { #wait on cosumers to come up
            #yield();
            sleep($thread_sleep_secs);
        }
        my $state = $RUNNING; #start at first
        while (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0) { #as long there is one running consumer and no defunct consumer
            fetching_rows($reader_db,$context->{tablename},$i,$context->{blocksize},$context->{rowcount},getlogger(__PACKAGE__));
            my $rowblock = $reader_db->db_get_rowblock($context->{blocksize});
            my $realblocksize = scalar @$rowblock;
            my $packet = {rows     => $rowblock,
                          size     => $realblocksize,
                          #block    => $i,
                          row_offset => $i};
            my %packet :shared = ();
            $packet{rows} = $rowblock;
            $packet{size} = $realblocksize;
            $packet{row_offset} = $i;
            if ($realblocksize > 0) {
                $context->{queue}->enqueue(\%packet); #$packet);
                $blockcount++;
                #wait if thequeue is full and there there is one running consumer
                while (((($state = _get_other_threads_state($context->{errorstates},$tid)) & $RUNNING) == $RUNNING) and $context->{queue}->pending() >= $context->{threadqueuelength}) {
                    #yield();
                    sleep($thread_sleep_secs);
                }
                $i += $realblocksize;
                if ($realblocksize < $context->{blocksize}) {
                    tablethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                $context->{queue}->enqueue(\%packet); #$packet);
                tablethreadingdebug('[' . $tid . '] reader thread is shutting down (end of data - empty block) ...',getlogger(__PACKAGE__));
                last;
            }
        }
        if (not (($state & $RUNNING) == $RUNNING and ($state & $ERROR) == 0)) {
            tablethreadingdebug('[' . $tid . '] reader thread is shutting down (' .
                              (($state & $RUNNING) == $RUNNING ? 'still running consumer threads' : 'no running consumer threads') . ', ' .
                              (($state & $ERROR) == 0 ? 'no defunct thread(s)' : 'defunct thread(s)') . ') ...'
            ,getlogger(__PACKAGE__));
        }
        $reader_db->db_finish();
    };
    # stop the consumer:
    # $context->{queue}->enqueue(undef);
    if (defined $reader_db) {
        # if thread cleanup has a problem...
        $reader_db->db_disconnect();
    }
    tablethreadingdebug($@ ? '[' . $tid . '] reader thread error: ' . $@ : '[' . $tid . '] reader thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub _writer {

    my $context = shift;

    #get_target_db
    my $writer_db;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }
    tablethreadingdebug('[' . $tid . '] writer thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        $writer_db = &{$context->{get_target_db}}(); #$writer_connection_name);
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {
                    writing_rows($writer_db,$context->{targettablename},$packet->{row_offset},$packet->{size},$context->{rowcount},getlogger(__PACKAGE__));

                    $writer_db->db_do_begin($context->{insertstatement},$context->{targettablename});
                    $writer_db->db_do_rowblock($packet->{rows});
                    $writer_db->db_finish();
                    $blockcount++;

                } else { #empty packet received
                    tablethreadingdebug('[' . $tid . '] shutting down writer thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    if (defined $writer_db) {
        # if thread cleanup has a problem...
        $writer_db->db_disconnect();
    }
    tablethreadingdebug($@ ? '[' . $tid . '] writer thread error: ' . $@ : '[' . $tid . '] writer thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub _process {

    my $context = shift;

    #my $writer_db;
    my $rowblock_result = 1;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }

    tablethreadingdebug('[' . $tid . '] processor thread tid ' . $tid . ' started',getlogger(__PACKAGE__));

    my $blockcount = 0;
    eval {
        #$writer_db = &{$context->{get_target_db}}($writer_connection_name);
        while (not _get_stop_consumer_thread($context,$tid)) {
            my $packet = $context->{queue}->dequeue_nb();
            if (defined $packet) {
                if ($packet->{size} > 0) {

                    #writing_rows($writer_db,$context->{targettablename},$i,$realblocksize,$context->{rowcount},getlogger(__PACKAGE__));

                    #$writer_db->db_do_begin($context->{insertstatement},$context->{targettablename});
                    #$writer_db->db_do_rowblock($rowblock);
                    #$writer_db->db_finish();

                    #$i += $realblocksize;

                    processing_rows($tid,$packet->{row_offset},$packet->{size},$context->{rowcount},getlogger(__PACKAGE__));

                    $rowblock_result = &{$context->{process_code}}($packet->{rows},$packet->{row_offset});

                    $blockcount++;

                    #$i += $realblocksize;

                    if (not $rowblock_result) {
                        tablethreadingdebug('[' . $tid . '] shutting down processor thread (processing block NOK) ...',getlogger(__PACKAGE__));
                        last;
                    }

                } else {
                    tablethreadingdebug('[' . $tid . '] shutting down processor thread (end of data - empty block) ...',getlogger(__PACKAGE__));
                    last;
                }
            } else {
                #yield();
                sleep($thread_sleep_secs); #2015-01
            }
        }
    };
    #if (defined $writer_db) {
    #    $writer_db->db_disconnect();
    #}
    tablethreadingdebug($@ ? '[' . $tid . '] processor thread error: ' . $@ : '[' . $tid . '] processor thread finished (' . $blockcount . ' blocks)',getlogger(__PACKAGE__));
    lock $context->{errorstates};
    if ($@) {
        $context->{errorstates}->{$tid} = $ERROR;
    } else {
        $context->{errorstates}->{$tid} = (not $rowblock_result) ? $ERROR : $COMPLETED;
    }
    return $context->{errorstates}->{$tid};
}

sub copy_row {
    my ($record,$row,$expected_fieldnames) = @_;
    if (defined $record and defined $row) {
        my $i;
        if (ref $row eq 'ARRAY') {
            $i = 0;
        } elsif (ref $row eq 'HASH') {
            $i = -1;
        } elsif (ref $row eq ref $record) {
            $i = -2;
        } else {
            $i = -3;
        }
        foreach my $fieldname (@$expected_fieldnames) {
            if ($i >= 0) {
                $record->{$fieldname} = $row->[$i];
                $i++;
            } elsif ($i == -1 or $i == -2) {
                if (exists $row->{$fieldname}) {
                    $record->{$fieldname} = $row->{$fieldname};
                } elsif (exists $row->{uc($fieldname)}) {
                    $record->{$fieldname} = $row->{uc($fieldname)};
                } else {
                    $record->{$fieldname} = undef;
                }
            } else {
                last;
            }
        }
    }
    return $record;
}

1;
