package NGCP::BulkProcessor::SqlConnectors::SQLServerDB;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use NGCP::BulkProcessor::Globals qw($LongReadLen_limit);
use NGCP::BulkProcessor::Logging qw(
    getlogger
    dbinfo
    dbdebug
    texttablecreated
    temptablecreated
    indexcreated
    primarykeycreated
    tabletruncated
    tabledropped);
use NGCP::BulkProcessor::LogError qw(
    dberror
    fieldnamesdiffer);

use DBI;
use DBD::ODBC 1.50;

#https://blog.afoolishmanifesto.com/posts/install-and-configure-the-ms-odbc-driver-on-debian/
#http://community.spiceworks.com/how_to/show/78224-install-the-ms-sql-odbc-driver-on-debian-7

use NGCP::BulkProcessor::Array qw(arrayeq itemcount contains setcontains removeduplicates mergearrays);

use NGCP::BulkProcessor::SqlConnector;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlConnector);
our @EXPORT_OK = qw(get_tableidentifier);

my $defaulthost = '127.0.0.1';
my $defaultport = '1433';
my $defaultusername = 'SA';
my $defaultpassword = '';
my $defaultdatabasename = 'master';

my $varcharsize = 900; #8000;

my $encoding = 'LATIN1';
my $collation_name = 'Latin1_General_CI_AS'; #OS locales only
my $lc_ctype = 'C';

my $client_encoding = 'LATIN1';

my $LongReadLen = $LongReadLen_limit; #bytes
my $LongTruncOk = 0;

#my $logger = getlogger(__PACKAGE__);

my $lock_do_chunk = 0;
my $lock_get_chunk = 0;

my $transaction_isolation_level = ''; #'SERIALIZABLE'

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::SqlConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{databasename} = undef;
    $self->{username} = undef;
    $self->{password} = undef;

    $self->{drh} = DBI->install_driver('ODBC');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',getlogger(__PACKAGE__));

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    if (defined $self->{databasename}) {
        return $self->{username} . '@' . $self->{host} . ':' . $self->{port} . '.' . $self->{databasename};
    } else {
        return undef;
    }

}

sub tableidentifier {

    my $self = shift;
    my $tablename = shift;
    return $tablename;
}

sub columnidentifier {

    my $self = shift;
    my $columnname = shift;

    return $columnname;

}

sub get_tableidentifier {

    my ($tablename,$databasename) = @_;

    if (defined $databasename) {
        return $databasename . '.' . $tablename;
    } else {
        return $tablename;
    }

}

sub getsafetablename {

    # make a table name (identifier) string save for use within create table statements
    # of this rdbms connector.
    my $self = shift;
    my $tableidentifier = shift;

    return lc($self->SUPER::getsafetablename($tableidentifier));

}

sub paginate_sort_query {
    my $self = shift;
    my $statement = shift;
    my $offset = shift;
    my $limit = shift;
    my $sortingconfigurations = shift;

    my $orderby = $self->_orderby_columns($sortingconfigurations);
    if (defined $offset and defined $limit and length($orderby) > 0) {
        my ($select_fields_part,$table_whereclause_part) = split /\s+from\s+/i,$statement,2;
        $select_fields_part =~ s/^\s*select\s+//i;

        return 'SELECT * FROM (SELECT ' . $select_fields_part . ', ROW_NUMBER() OVER (ORDER BY ' . $orderby . ') as row FROM ' . $table_whereclause_part . ') AS p WHERE p.row > ' . $offset . ' AND p.row <= ' . ($offset + $limit);
    }

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    return 'TRY_CONVERT(NUMERIC, ' . $column . ')';
}

sub _dbd_connect {
    my $self = shift;
    my $databasename = shift;
    my $connection_string;
    if ($^O eq 'MSWin32') {
        $connection_string = 'DBI:ODBC:Driver={SQL Server};Server=' . $self->{host} . ',' . $self->{port};
    } else {
        $connection_string = 'dbi:ODBC:driver=SQL Server Native Client 11.0;server=tcp:' . $self->{host} . ',' . $self->{port}; # . ';database=DB_TOWNE;MARS_Connection=yes;
    }
    if (length($databasename) > 0) {
        $connection_string .= ';database=' . $databasename;
    }
    return (DBI->connect($connection_string,$self->{username},$self->{password},
            {
                PrintError      => 0,
                RaiseError      => 0,
                AutoCommit      => 1,
                #AutoCommit      => 0,
            }
        ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),getlogger(__PACKAGE__)));
}

sub getdatabases {

    my $self = shift;

    my $connected_wo_db = 0;
    if (!defined $self->{dbh}) {
       $self->{dbh} = $self->_dbd_connect();
       $connected_wo_db = 1;
    }
    my $dbs = $self->db_get_col('SELECT name FROM master..sysdatabases');
    if ($connected_wo_db) {
        $self->{dbh}->disconnect() or dberror($self,'error disconnecting: ' . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
        $self->{dbh} = undef;
    }

    return $dbs;

}

sub _createdatabase {

    my $self = shift;
    my ($databasename) = @_;

        $self->{dbh} = $self->_dbd_connect();
        $self->db_do('CREATE DATABASE ' . $databasename . ' COLLATE ' . $collation_name);
        dbinfo($self,'database \'' . $databasename . '\' created',getlogger(__PACKAGE__));
        $self->{dbh}->disconnect() or dberror($self,'error disconnecting: ' . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
        $self->{dbh} = undef;
}

sub db_connect {

    my $self = shift;

    my ($databasename,$username,$password,$host,$port) = @_;

    $self->SUPER::db_connect($databasename,$username,$password,$host,$port);

    #if (defined $self->{dbh}) {
    #    $self->db_disconnect();
    #}

    $host = $defaulthost if (not $host);
    $port = $defaultport if (not $port);
    $databasename = $defaultdatabasename if (not $databasename);
    $username = $defaultusername if (not $username);
    $password = $defaultpassword if (not $password);

    $self->{host} = $host;
    $self->{port} = $port;
    $self->{databasename} = $databasename;
    $self->{username} = $username;
    $self->{password} = $password;

    if (not contains($databasename,$self->getdatabases(),0)) {
        $self->_createdatabase($databasename);
    }

    dbdebug($self,'connecting',getlogger(__PACKAGE__));

    my $dbh = $self->_dbd_connect($databasename);

    $dbh->{InactiveDestroy} = 1;

    $dbh->{LongReadLen} = $LongReadLen;
    $dbh->{LongTruncOk} = $LongTruncOk;

    $self->{dbh} = $dbh;

    #$self->db_do('SET CLIENT_ENCODING TO ?',$client_encoding);

    if (length($transaction_isolation_level) > 0) {
        $self->db_do('SET TRANSACTION ISOLATION LEVEL ' . $transaction_isolation_level);
    }

    dbinfo($self,'connected',getlogger(__PACKAGE__));

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('DBCC SHRINKDATABASE (0) WITH NO_INFOMSGS');

}

sub _db_disconnect {

    my $self = shift;

    $self->SUPER::_db_disconnect();

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;
    return $self->db_get_col('SELECT column_name FROM information_schema.columns WHERE table_name = ?',$tablename);
    #return $self->db_get_col('SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?)', 'dbo.' . $tablename);

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;
    return $self->db_get_col('SELECT c.column_name from ' .
        'information_schema.table_constraints t, ' .
        'information_schema.constraint_column_usage c ' .
        'WHERE ' .
            'c.constraint_name = t.constraint_name ' .
            'AND c.table_name = t.table_name ' .
            'AND t.constraint_type = ? ' .
            'AND c.table_name = ?','PRIMARY KEY',$tablename);

}

sub create_temptable {

    my $self = shift;
    my $select_stmt = shift;
    my $indexes = shift;

    my $index_tablename = $self->_gettemptablename();
    my $temp_tablename = '##' . $index_tablename;

    my ($select_fields_part,$table_whereclause_part) = split /\s+from\s+/i,$select_stmt,2;

    $self->db_do($select_fields_part . ' INTO ' . $temp_tablename . ' FROM ' . $table_whereclause_part);
    temptablecreated($self,$index_tablename,getlogger(__PACKAGE__));

    if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
        foreach my $indexname (keys %$indexes) {
            my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
            #if (not arrayeq($indexcols,$keycols,1)) {
                #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                $indexname = lc($index_tablename) . '_' . $indexname;
                $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $temp_tablename . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
                indexcreated($self,$index_tablename,$indexname,getlogger(__PACKAGE__));
            #}
        }
    }

    return $temp_tablename;

}

sub create_primarykey {

    my $self = shift;
    my ($tablename,$keycols,$fieldnames) = @_;

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        if (defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
            my $statement = 'ALTER TABLE ' . $self->tableidentifier($tablename) . ' ADD PRIMARY KEY (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$keycols) . ')';
            $self->db_do($statement);
            primarykeycreated($self,$tablename,$keycols,getlogger(__PACKAGE__));
            return 1;
        }

    }

    return 0;
}
sub create_indexes {

    my $self = shift;
    my ($tablename,$indexes,$keycols) = @_;

    my $index_count = 0;
    if (length($tablename) > 0) {

            if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
                    if (not arrayeq($indexcols,$keycols,1)) {
                        #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                        $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $self->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
                        indexcreated($self,$tablename,$indexname,getlogger(__PACKAGE__));
                    }
                }
            }

    }

    return $index_count;
}

sub create_texttable {

    my $self = shift;
    my ($tablename,$fieldnames,$keycols,$indexes,$truncate,$defer_indexes) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);
    #my ($tableidentifier,$fieldnames,$keycols,$indexes,$truncate) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        my $created = 0;
        if ($self->table_exists($tablename) == 0) {
            my $statement = 'CREATE TABLE ' . $self->tableidentifier($tablename) . ' (';
            #$statement .= join(' TEXT, ',@$fieldnames) . ' TEXT';

            my $allindexcols = [];
            if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    $allindexcols = mergearrays($allindexcols,$self->_extract_indexcols($indexes->{$indexname}));
                    #push(@allindexcols, $self->_extract_indexcols($indexes->{$indexname}));
                }
            }
            $allindexcols = removeduplicates($allindexcols,1);

            my @fieldspecs = ();
            foreach my $fieldname (@$fieldnames) {
                if (contains($fieldname,$keycols,1)) {
                    push @fieldspecs,$self->columnidentifier($fieldname) . ' VARCHAR(' . $varcharsize . ') NOT NULL';
                    #$statement .= $fieldname . ' VARCHAR(256)';
                } elsif (contains($fieldname,$allindexcols,1)) {
                    push @fieldspecs,$self->columnidentifier($fieldname) . ' VARCHAR(' . $varcharsize . ')';
                    #$statement .= $fieldname . ' VARCHAR(256)';
                } else {
                    push @fieldspecs,$self->columnidentifier($fieldname) . ' VARCHAR(MAX)';
                    #$statement .= $fieldname . ' TEXT';
                }
            }
            $statement .= join(', ',@fieldspecs);


            #if (defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
            if (not $defer_indexes and defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
                $statement .= ', PRIMARY KEY (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$keycols) . ')';
            }

            $statement .= ')'; # CHARACTER SET ' . $texttable_charset . ', COLLATE ' . $texttable_collation . ', ENGINE ' . $texttable_engine;

            $self->db_do($statement);
            texttablecreated($self,$tablename,getlogger(__PACKAGE__));

            if (not $defer_indexes and defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
                    if (not arrayeq($indexcols,$keycols,1)) {
                        #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                        $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $self->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
                        indexcreated($self,$tablename,$indexname,getlogger(__PACKAGE__));
                    }
                }
            }

            $created = 1;
        } else {
            my $fieldnamesfound = $self->getfieldnames($tablename);
            if (not setcontains($fieldnames,$fieldnamesfound,1)) {
                fieldnamesdiffer($self,$tablename,$fieldnames,$fieldnamesfound,getlogger(__PACKAGE__));
                return 0;
            }
        }

        if (not $created and $truncate) {
            $self->truncate_table($tablename);
        }
        return 1;
    } else {
        return 0;
    }

    #return $tablename;

}

sub multithreading_supported {

    my $self = shift;
    return 1;

}

sub truncate_table {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('TRUNCATE ' . $self->tableidentifier($tablename));
    tabletruncated($self,$tablename,getlogger(__PACKAGE__));

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    return $self->db_get_value('SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?',$tablename);

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    if ($self->table_exists($tablename) > 0) {
        $self->db_do('DROP TABLE ' . $self->tableidentifier($tablename));
        tabledropped($self,$tablename,getlogger(__PACKAGE__));
        return 1;
    }
    return 0;

}

sub db_do_begin {

    my $self = shift;
    my $query = shift;
    my $tablename = shift;

    $self->SUPER::db_do_begin($query,$tablename,$lock_do_chunk,@_);

}

sub db_get_begin {

    my $self = shift;
    my $query = shift;
    my $tablename = shift;
    #my $lock = shift;

    $self->SUPER::db_get_begin($query,$tablename,$lock_get_chunk,@_);

}

sub db_finish {

    my $self = shift;
    #my $unlock = shift;

    $self->SUPER::db_finish($lock_do_chunk | $lock_get_chunk);

}

1;
