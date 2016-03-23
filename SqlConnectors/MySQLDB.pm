package SqlConnectors::MySQLDB;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Globals qw($LongReadLen_limit);
use Logging qw(
    getlogger
    dbinfo
    dbdebug
    texttablecreated
    temptablecreated
    indexcreated
    primarykeycreated
    tabletruncated
    tabledropped);
use LogError qw(dberror fieldnamesdiffer);

use DBI;
use DBD::mysql 4.014;

use Array qw(arrayeq itemcount contains setcontains);

use SqlConnector;

require Exporter;
our @ISA = qw(Exporter SqlConnector);
our @EXPORT_OK = qw(get_tableidentifier);

my $defaulthost = '127.0.0.1';
my $defaultport = '3306';
my $defaultusername = 'root';
my $defaultpassword = '';
my $defaultdatabasename = 'test';

my $varcharsize = 256;

my $texttable_charset = 'latin1';
my $texttable_collation = 'latin1_swedish_ci';
my $default_texttable_engine = 'MyISAM'; #InnoDB'; # ... provide transactions y/n?

my $session_charset = 'latin1';

my $LongReadLen = $LongReadLen_limit; #bytes
my $LongTruncOk = 0;

my $logger = getlogger(__PACKAGE__);

my $lock_do_chunk = 1;
my $lock_get_chunk = 0;

my $serialization_level = ''; #'SERIALIZABLE'

sub new {

    my $class = shift;

    my $self = SqlConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{databasename} = undef;
    $self->{username} = undef;
    $self->{password} = undef;

    $self->{drh} = DBI->install_driver('mysql');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',$logger);

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

    if (defined $self->{databasename}) {
        return '`' . $self->{databasename} . '`.`' . $tablename . '`';
    } else {
        return '`' . $tablename . '`';
    }

}

sub columnidentifier {

    my $self = shift;
    my $columnname = shift;

    return '`' . $columnname . '`';

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

    my $self = shift;
    my $tableidentifier = shift;

    return lc($self->SUPER::getsafetablename($tableidentifier));

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    return '(' . $column . ' + 0.0)';
}

sub getdatabases {

    my $self = shift;

    my @dbs = $self->{drh}->func($self->{host},
                                 $self->{port},
                                 $self->{username},
                                 $self->{password},
                                 '_ListDBs') or
        dberror($self,'error listing databases: ' . $self->{drh}->errstr(),$logger);

    return \@dbs;

}

sub _createdatabase {

    my $self = shift;
    my ($databasename) = @_;

    if ($self->{drh}->func('createdb',
                           $databasename,
                           'host=' . $self->{host} . ';port=' . $self->{port},
                           $self->{username},
                           $self->{password},
                           'admin')) {
        dbinfo($self,'database \'' . $databasename . '\' created',$logger);
    }
}

sub db_connect {

    my $self = shift;

    my ($databasename,$username,$password,$host,$port) = @_;

    $self->SUPER::db_connect($databasename,$username,$password,$host,$port);

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

    dbdebug($self,'connecting',$logger);

    my $dbh = DBI->connect(
        'dbi:mysql:database=' . $databasename . ';host=' . $host . ';port=' . $port,$username,$password,
        {
            PrintError      => 0,
            RaiseError      => 0,
            AutoCommit      => 1,
            #AutoCommit      => 0,
        }
    ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),$logger);

    $dbh->{InactiveDestroy} = 1;

    $dbh->{LongReadLen} = $LongReadLen;
    $dbh->{LongTruncOk} = $LongTruncOk;

    $self->{dbh} = $dbh;

    my $server_version = substr($self->db_get_all_hashref('SHOW VARIABLES LIKE \'version\'','Variable_name')->{version}->{Value},0,2);
    if ($server_version ge '4.1') {
    #    $self->db_do('SET SESSION character_set_client = \'utf8\'');
    #    $self->db_do('SET SESSION character_set_connection = \'utf8\'');
    #    $self->db_do('SET SESSION character_set_results = \'utf8\'');
        $self->db_do('SET CHARACTER SET ' . $session_charset . '');
        dbdebug($self,'session charset ' . $session_charset . ' applied',$logger);
    } else {
    #    $self->db_do('SET SESSION CHARACTER SET = \'utf8\'');
        #$self->db_do('SET SESSION CHARACTER SET = \'latin1\'');

        #$self->db_do('SET SESSION CHARACTER SET \'cp1251_koi8\''); # the only valid if convert.cc on server is not modified
    }


    #$self->db_do('SET character_set_client = \'utf8\'');
    #$self->db_do('SET character_set_connection = \'utf8\'');
    #$self->db_do('SET character_set_results = \'utf8\'');
    #$self->db_do('SET SESSION NAMES = \'utf8\'');
    #$self->db_do('SET character_set_connection = \'utf8\'');
    #$self->db_do('SET character_set_results = \'utf8\'');

    #$self->db_do('SET SESSION date_format = \'%Y-%m-%d\'');
    #$self->db_do('SET SESSION time_format = \'%H:%i:%s\'');
    #$self->db_do('SET SESSION time_zone = \'Europe/Paris\'');
    #$self->db_do('SET SESSION datetime_format = \'%Y-%m-%d %H:%i:%s\'');

    if (length($serialization_level) > 0) {
        $self->db_do('SET SESSION TRANSACTION ISOLATION LEVEL ' . $serialization_level);
    }

    dbinfo($self,'connected',$logger);

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('OPTIMIZE TABLE ' . $self->tableidentifier($tablename));

}

sub _db_disconnect {

    my $self = shift;

    $self->SUPER::_db_disconnect();

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;
    return $self->db_get_col('SHOW FIELDS FROM ' . $self->tableidentifier($tablename));

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;
    my $fieldinfo = $self->db_get_all_hashref('SHOW FIELDS FROM ' . $self->tableidentifier($tablename),'Field');
    my @keycols = ();
    foreach my $fieldname (keys %$fieldinfo) {
        if (uc($fieldinfo->{$fieldname}->{'Key'}) eq 'PRI') {
            push @keycols,$fieldname;
        }
    }
    return \@keycols;

}

sub create_temptable {

    my $self = shift;
    my $select_stmt = shift;
    my $indexes = shift;

    my $index_tablename = $self->_gettemptablename();
    my $temp_tablename = $self->tableidentifier($index_tablename);

    $self->db_do('CREATE TEMPORARY TABLE ' . $temp_tablename . ' AS ' . $select_stmt);
    temptablecreated($self,$index_tablename,$logger);

    if (defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
        foreach my $indexname (keys %$indexes) {
            #my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
            #if (not arrayeq($indexcols,$keycols,1)) {
                #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                my $temptable_indexname = lc($index_tablename) . '_' . $indexname;
                $self->db_do('CREATE INDEX ' . $temptable_indexname . ' ON ' . $temp_tablename . ' (' . join(', ', map { local $_ = $_; my @indexcol = _split_indexcol($_); $_ = $self->columnidentifier($indexcol[0]) . $indexcol[1]; $_; } @{$indexes->{$indexname}}) . ')');
                indexcreated($self,$index_tablename,$indexname,$logger);
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
            my $statement = 'ALTER TABLE ' . $self->tableidentifier($tablename) . ' ADD PRIMARY KEY (' . join(', ',map { local $_ = $_; my @indexcol = _split_indexcol($_); $_ = $self->columnidentifier($indexcol[0]) . $indexcol[1]; $_; } @$keycols) . ')';
            $self->db_do($statement);
            primarykeycreated($self,$tablename,$keycols,$logger);
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
                if (not arrayeq($self->_extract_indexcols($indexes->{$indexname}),$keycols,1)) {
                    my $statement = 'CREATE INDEX ' . $indexname . ' ON ' . $self->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; my @indexcol = _split_indexcol($_); $_ = $self->columnidentifier($indexcol[0]) . $indexcol[1]; $_; } @{$indexes->{$indexname}}) . ')';
                    $self->db_do($statement);
                    indexcreated($self,$tablename,$indexname,$logger);
                    $index_count++;
                }
            }
        }

    }

    return $index_count;
}

sub create_texttable {

    my $self = shift;
    my ($tablename,$fieldnames,$keycols,$indexes,$truncate,$defer_indexes,$texttable_engine) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);
    #my ($tableidentifier,$fieldnames,$keycols,$indexes,$truncate) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        my $created = 0;
        if ($self->table_exists($tablename) == 0) {
            my $statement = 'CREATE TABLE ' . $self->tableidentifier($tablename) . ' (';
            #$statement .= join(' TEXT, ',@$fieldnames) . ' TEXT';

            my @fieldspecs = ();
            foreach my $fieldname (@$fieldnames) {
                if (contains($fieldname,$keycols,1)) {
                    push @fieldspecs,$self->columnidentifier($fieldname) . ' VARCHAR(' . $varcharsize . ')';
                    #$statement .= $fieldname . ' VARCHAR(256)';
                } else {
                    push @fieldspecs,$self->columnidentifier($fieldname) . ' TEXT';
                    #$statement .= $fieldname . ' TEXT';
                }
            }
            $statement .= join(', ',@fieldspecs);


            #if (defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
            if (not $defer_indexes and defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
                $statement .= ', PRIMARY KEY (' . join(', ',map { local $_ = $_; my @indexcol = _split_indexcol($_); $_ = $self->columnidentifier($indexcol[0]) . $indexcol[1]; $_; } @$keycols) . ')';
            }
            if (not $defer_indexes and defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    if (not arrayeq($self->_extract_indexcols($indexes->{$indexname}),$keycols,1)) {
                        $statement .= ', INDEX ' . $indexname . ' (' . join(', ',map { local $_ = $_; my @indexcol = _split_indexcol($_); $_ = $self->columnidentifier($indexcol[0]) . $indexcol[1]; $_; } @{$indexes->{$indexname}}) . ')';
                    }
                }
            }
            if (length($texttable_engine) == 0) {
                $texttable_engine = $default_texttable_engine;
            }
            $statement .= ') CHARACTER SET ' . $texttable_charset . ', COLLATE ' . $texttable_collation . ', ENGINE ' . $texttable_engine;

            $self->db_do($statement);
            texttablecreated($self,$tablename . ' (' . $texttable_engine . ')',$logger);
            $created = 1;
        } else {
            my $fieldnamesfound = $self->getfieldnames($tablename);
            if (not setcontains($fieldnames,$fieldnamesfound,1)) {
                fieldnamesdiffer($self,$tablename,$fieldnames,$fieldnamesfound,$logger);
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

sub truncate_table {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('TRUNCATE ' . $self->tableidentifier($tablename));
    tabletruncated($self,$tablename,$logger);

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    # ... again, avoid using mysql's information_schema table,
    # since its availability is obviously user/version dependent.
    return itemcount($tablename,$self->db_get_col('SHOW TABLES')); #,1);

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    if ($self->table_exists($tablename) > 0) {
        $self->db_do('DROP TABLE ' . $self->tableidentifier($tablename));
        tabledropped($self,$tablename,$logger);
        return 1;
    }
    return 0;

}


sub lock_tables {

    my $self = shift;
    my $tablestolock = shift;

    if (defined $self->{dbh} and defined $tablestolock and ref $tablestolock eq 'HASH') {

       my $locks = join(', ',map { local $_ = $_; $_ = $self->tableidentifier($_) . ' ' . $tablestolock->{$_}; $_; } keys %$tablestolock);
       dbdebug($self,"lock_tables:\n" . $locks,$logger);
       $self->db_do('LOCK TABLES ' . $locks);

    }

}

sub unlock_tables {

    my $self = shift;
    if (defined $self->{dbh}) {

       dbdebug($self,'unlock_tables',$logger);
       $self->db_do('UNLOCK TABLES');

    }

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

sub _split_indexcol {
    my $indexcol = shift;
    if ($indexcol =~ /(.+)(\(\d+\))/g) {
        return ($1,$2);
    }
    return ($indexcol, '');
}

1;