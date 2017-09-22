package NGCP::BulkProcessor::SqlConnectors::OracleDB;
use strict;

## no critic

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
use NGCP::BulkProcessor::LogError qw(dberror dbwarn fieldnamesdiffer);

use DBI;
use DBD::Oracle 1.21;

use NGCP::BulkProcessor::Array qw(contains arrayeq setcontains);

use NGCP::BulkProcessor::SqlConnector;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlConnector);
our @EXPORT_OK = qw(get_tableidentifier);

my $defaultport = '1521';

#$NLS_LANG = 'GERMAN_AUSTRIA.WE8ISO8859P1';

my $connNLS_LANGUAGE = 'GERMAN';
my $connNLS_TERRITORY = 'AUSTRIA';

#my $connNLS_CHARACTERSET = 'WE8ISO8859P1';

my $varcharsize = 4000;
my $max_identifier_length = 30;

my $LongReadLen = $LongReadLen_limit; #bytes
my $LongTruncOk = 0;

#my $logger = getlogger(__PACKAGE__);

#my $lock_do_chunk = 0;
#my $lock_get_chunk = 0;

my $rowblock_transactional = 1;

my $isolation_level = ''; #'SERIALIZABLE'

my $enable_numeric_sorting = 0;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::SqlConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{servicename} = undef;
    $self->{sid} = undef;
    $self->{username} = undef;
    $self->{password} = undef;
    $self->{schema} = undef;

    $self->{drh} = DBI->install_driver('Oracle');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',getlogger(__PACKAGE__));

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    if (defined $self->{schema}) {
        if ($self->{servicename}) {
            return $self->{username} . '@' . $self->{host} . ':' . $self->{port} . '/' . $self->{servicename} . '.' . $self->{schema};
        } elsif ($self->{sid}) {
            return $self->{username} . '@' . $self->{host} . ':' . $self->{port} . '/SID ' . $self->{sid} . '.' . $self->{schema};
        } else {
            return undef;
        }
    } else {
        return undef;
    }


}

sub tableidentifier {

    my $self = shift;
    my $tablename = shift;
    return get_tableidentifier($tablename,$self->{schema});

}

sub columnidentifier {

    my $self = shift;
    my $columnname = shift;

    return $columnname;

}

sub _chopidentifier {
    my $identifier = shift;
    return substr($identifier,0,$max_identifier_length);
}

sub get_tableidentifier {

    my ($tablename,$schema,$servicename,$sid) = @_;
    my $tableidentifier = $tablename;
    if (defined $schema) {
        $tableidentifier = $schema . '.' . $tableidentifier;
    }
    if ($servicename) {
        $tableidentifier = $servicename . '.' . $tableidentifier;
    } elsif ($sid) {
        $tableidentifier = $sid . '.' . $tableidentifier;
    }
    return $tableidentifier;

}

sub getsafetablename {

    my $self = shift;
    my $tableidentifier = shift;
    return uc($self->SUPER::getsafetablename($tableidentifier));
    #if (defined $self->{schema}) {
    #    return $self->{schema} . '.' . $tablename;
    #} else {
    #    return $tablename;
    #}

}

sub paginate_sort_query {

    my $self = shift;
    my $statement = shift;
    my $offset = shift;
    my $limit = shift;
    my $sortingconfigurations = shift;

    my $orderby = $self->_orderby_columns($sortingconfigurations);
    if (length($orderby) > 0) {
        $statement .= ' ORDER BY ' . $orderby;
    }
    if (defined $offset and defined $limit) {
        $statement = 'SELECT * FROM (SELECT p.*, rownum rnum FROM (' . $statement . ') p WHERE rownum < ' . ($offset + $limit + 1) . ') WHERE rnum >= ' . ($offset + 1);
    }
    return $statement;

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    return 'try_to_number(' . $column . ')';
}

sub getdatabases {

    my $self = shift;
    return $self->db_get_col('SELECT DISTINCT owner FROM all_objects');

}

#sub _createdatabase {
#
#    my $self = shift;
#    my ($schema) = @_;
#
#    #SQL> create tablespace test datafile 'C:\oraclexe\app\oracle\oradata\XE\test.dbf' size 10M autoextend on;
#    #Tablespace created.
#    #SQL> create user test identified by test default tablespace test;
#    #User created.
#    #alter user test quota unlimited on test
#
#    $self->db_do('CREATE SCHEMA AUTHORIZATION ' . $schema);
#    dbinfo($self,'schema \'' . $schema . '\' created',getlogger(__PACKAGE__));
#
#}

sub db_connect {

    my $self = shift;

    my ($servicename,$sid,$schema,$username,$password,$host,$port) = @_;

    $self->SUPER::db_connect($servicename,$sid,$schema,$username,$password,$host,$port);

    $port = $defaultport if (not $port);

    $self->{host} = $host;
    $self->{port} = $port;
    $self->{servicename} = $servicename;
    $self->{sid} = $sid;
    $self->{username} = $username;
    $self->{password} = $password;
    $self->{schema} = $schema;

    dbdebug($self,'connecting',getlogger(__PACKAGE__));

    my $dbh;
    if ($servicename) {
        $dbh = DBI->connect(
            'dbi:Oracle:host=' . $host . ';service_name=' . $servicename . ';port=' . $port,$username,$password,
            {
                PrintError      => 0,
                RaiseError      => 0,
                AutoCommit      => 1,
                #AutoCommit      => 0,
            }
        ) or dberror($self,'error connecting - service name: ' . $self->{drh}->errstr(),getlogger(__PACKAGE__));
    } elsif ($sid) {
        $dbh = DBI->connect(
            'dbi:Oracle:host=' . $host . ';sid=' . $sid . ';port=' . $port,$username,$password,
            {
                PrintError      => 0,
                RaiseError      => 0,
                AutoCommit      => 1,
            }
        ) or dberror($self,'error connecting - sid: ' . $self->{drh}->errstr(),getlogger(__PACKAGE__));
    } else {
        dberror($self,'neither service name nor sid specified',getlogger(__PACKAGE__));
    }

    $self->{dbh} = $dbh;

    if (not contains($schema,$self->getdatabases(),0)) {
        $self->_createdatabase($schema); #notimplemented error...
    }

    $dbh->{InactiveDestroy} = 1;

    $dbh->{LongReadLen} = $LongReadLen;
    $dbh->{LongTruncOk} = $LongTruncOk;

    $self->db_do('ALTER SESSION SET NLS_LANGUAGE = \'' . $connNLS_LANGUAGE . '\'');
    $self->db_do('ALTER SESSION SET NLS_TERRITORY = \'' . $connNLS_TERRITORY . '\'');
    #$self->db_do('ALTER SESSION SET NLS_CHARACTERSET = \'' . $connNLS_CHARACTERSET . '\'');
    $self->db_do('ALTER SESSION SET NLS_NUMERIC_CHARACTERS = \'.,\'');
    $self->db_do('ALTER SESSION SET NLS_DATE_FORMAT = \'YYYY-MM-DD HH24:MI:SS\'');

    if (length($isolation_level) > 0) {
        $self->db_do('ALTER SESSION SET ISOLATION_LEVEL = ' . $isolation_level);
    }

    if ($enable_numeric_sorting) {
        #http://stackoverflow.com/questions/6470819/sql-if-cannot-convert-to-number-set-as-null
        eval {
            $self->db_do("CREATE OR REPLACE FUNCTION try_to_number( p_str IN VARCHAR2 )\n" .
            "  RETURN NUMBER\n" .
            "IS\n" .
            "  l_num NUMBER;\n" .
            "BEGIN\n" .
            "  BEGIN\n" .
            "    l_num := to_number( p_str );\n" .
            "  EXCEPTION\n" .
            "    WHEN others THEN\n" .
            "      l_num := null;\n" .
            "  END;\n" .
            "  RETURN l_num;\n" .
            "END;");
        };
        if ($@) {
            dbwarn($self,'numeric sorting not supported',getlogger(__PACKAGE__));
        }
    } else {
        dbdebug($self,'numeric sorting not enabled',getlogger(__PACKAGE__));
    }

    dbinfo($self,'connected',getlogger(__PACKAGE__));

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

}

sub _db_disconnect {

    my $self = shift;

    $self->SUPER::_db_disconnect();

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;
    return $self->db_get_col('SELECT column_name FROM all_tab_columns WHERE CONCAT(CONCAT(owner,\'.\'),table_name) = ?',$self->tableidentifier($tablename));

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;
    return $self->db_get_col('SELECT cols.column_name FROM all_constraints cons, all_cons_columns cols WHERE
                                CONCAT(CONCAT(cols.owner,\'.\'),cols.table_name) = ? AND
                                cons.constraint_type = \'P\' AND
                                cons.constraint_name = cols.constraint_name AND
                                cons.owner = cols.owner
                                ORDER BY cols.table_name, cols.position',$self->tableidentifier($tablename));

}

sub create_temptable {

    my $self = shift;
    my $select_stmt = shift;
    my $indexes = shift;

    my $index_tablename = $self->_gettemptablename();
    my $temp_tablename = $self->tableidentifier($index_tablename);

    #$self->db_do('CREATE GLOBAL TEMPORARY TABLE ' . $temp_tablename . ' ON COMMIT PRESERVE ROWS AS ' . $select_stmt);
    $self->db_do('CREATE TABLE ' . $temp_tablename . ' AS ' . $select_stmt);
    push(@{$self->{temp_tables}},$index_tablename);

    temptablecreated($self,$index_tablename,getlogger(__PACKAGE__));

    #$self->{temp_table_count} += 1;

    if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
        foreach my $indexname (keys %$indexes) {
            my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
            #if (not arrayeq($indexcols,$keycols,1)) {
                #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                $indexname = _chopidentifier(lc($index_tablename) . '_' . $indexname);
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
                        $indexname = _chopidentifier($indexname);
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
    #my ($tableidentifier,$fieldnames,$keycols,$indexes,$truncate) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        my $created = 0;
        if ($self->table_exists($tablename) == 0) {
            my $statement = 'CREATE TABLE ' . $self->tableidentifier($tablename) . ' (';
            $statement .= join(' VARCHAR2(' . $varcharsize . '), ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$fieldnames) . ' VARCHAR2(' . $varcharsize . ')';
            if (not $defer_indexes and defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
                #$statement .= ', CONSTRAINT ' . $tablename . '_pk PRIMARY KEY (' . join(', ',@$keycols) . ')';
                $statement .= ', CONSTRAINT PRIMARY KEY (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$keycols) . ')';
            }
            #if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
            #    foreach my $indexname (keys %$indexes) {
            #        $statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
            #    }
            #}
            $statement .= ')';

            $self->db_do($statement);
            texttablecreated($self,$tablename,getlogger(__PACKAGE__));

            if (not $defer_indexes and defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
                    if (not arrayeq($indexcols,$keycols,1)) {
                        #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                        $indexname = _chopidentifier($indexname);
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

}

sub multithreading_supported {

    my $self = shift;
    return 1;

}

sub rowblock_transactional {

    my $self = shift;
    return $rowblock_transactional;

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

    return $self->db_get_value('SELECT COUNT(*) FROM all_tables WHERE CONCAT(CONCAT(owner,\'.\'),table_name) = ?',$self->tableidentifier($tablename));

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    if ($self->table_exists($tablename) > 0) {
        $self->db_do('DROP TABLE ' . $self->tableidentifier($tablename) . ' PURGE'); #CASCADE CONSTRAINTS PURGE');
        tabledropped($self,$tablename,getlogger(__PACKAGE__));
        return 1;
    }
    return 0;

}

sub db_do_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;

    $self->SUPER::db_do_begin($query,$rowblock_transactional,@_);

}

sub db_get_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;

    $self->SUPER::db_get_begin($query,$rowblock_transactional,@_);

}

sub db_finish {

    my $self = shift;
    #my $unlock = shift;
    my $rollback = shift;

    $self->SUPER::db_finish($rowblock_transactional,$rollback);

}

1;
