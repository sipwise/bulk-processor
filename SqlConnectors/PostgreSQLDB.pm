package SqlConnectors::PostgreSQLDB;
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
use LogError qw(dberror dbwarn fieldnamesdiffer);

use DBI;
use DBD::Pg 2.17.2;

use Array qw(arrayeq itemcount contains setcontains);

use SqlConnector;

require Exporter;
our @ISA = qw(Exporter SqlConnector);
our @EXPORT_OK = qw(get_tableidentifier);

my $defaulthost = '127.0.0.1';
my $defaultport = '5432';
my $defaultusername = 'postgres';
my $defaultpassword = '';
my $defaultschemaname = 'template1';

my $varcharsize = 256;

my $encoding = 'LATIN1';
my $lc_collate = 'C'; #OS locales only
my $lc_ctype = 'C';

my $client_encoding = 'LATIN1';

#my $LongReadLen = $LongReadLen_limit; #bytes
#my $LongTruncOk = 0;

my $logger = getlogger(__PACKAGE__);

my $lock_do_chunk = 0;
my $lock_get_chunk = 0;

#my $to_number_pattern = '9.9999999999999'; #EEEE';

my $transaction_isolation_level = ''; #'SERIALIZABLE'

my $enable_numeric_sorting = 0;

sub new {

    my $class = shift;

    my $self = SqlConnector->new(@_);

    $self->{host} = undef;
    $self->{port} = undef;
    $self->{schemaname} = undef;
    $self->{username} = undef;
    $self->{password} = undef;

    $self->{drh} = DBI->install_driver('Pg');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',$logger);

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    if (defined $self->{schemaname}) {
        return $self->{username} . '@' . $self->{host} . ':' . $self->{port} . '.' . $self->{schemaname};
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

    my ($tablename,$schemaname) = @_;

    #return SUPER::get_tableidentifier($tablename,$schemaname);

    if (defined $schemaname) {
        return $schemaname . '.' . $tablename;
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

    #$tableidentifier =~ s/[^0-9a-z_]/_/gi;
    #return lc($tableidentifier); # ... windows!

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
        $statement .= ' LIMIT ' . $limit . ' OFFSET ' . $offset;
    }
    return $statement;

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    return 'try_to_number(' . $column . '::text)'; # ,\'' . $to_number_pattern . '\')';
}

sub getdatabases {

    my $self = shift;

    my $DBI_USER = $ENV{DBI_USER};
    my $DBI_PASS = $ENV{DBI_PASS};
    $ENV{DBI_USER} = $self->{username};
    $ENV{DBI_PASS} = $self->{password};

    my @dbs = $self->{drh}->data_sources('port=' . $self->{port} . ';host=' . $self->{host});

    $DBI_USER = $ENV{DBI_USER};
    $DBI_PASS = $ENV{DBI_PASS};

    if (scalar @dbs == 0) {
        dberror($self,'error listing databases: ' . $self->{drh}->errstr(),$logger);
    } else {
        @dbs = map { local $_ = $_; $_ =~ s/^dbi:Pg:dbname=[\"\']?([a-zA-Z0-9_-]+)[\"\']?;.+$/$1/gi; $_; } @dbs;
    }

    return \@dbs;

}

sub _createdatabase {

    my $self = shift;
    my ($schemaname) = @_;

        my $dbh = DBI->connect(
            'dbi:Pg:database=template1;host=' . $self->{host} . ';port=' . $self->{port},$self->{username},$self->{password},
            {
                PrintError      => 0,
                RaiseError      => 0,
                AutoCommit      => 1,
                #AutoCommit      => 0,
            }
        ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),$logger);
        $self->{dbh} = $dbh;
        $self->db_do('CREATE DATABASE ' . $schemaname . ' TEMPLATE template0 ENCODING = ? LC_COLLATE = ? LC_CTYPE = ?', $encoding, $lc_collate, $lc_ctype);
        dbinfo($self,'database \'' . $schemaname . '\' created',$logger);
        $self->{dbh}->disconnect() or dberror($self,'error disconnecting: ' . $self->{dbh}->errstr(),$logger);
        $self->{dbh} = undef;
}

sub db_connect {

    my $self = shift;

    my ($schemaname,$username,$password,$host,$port) = @_;

    $self->SUPER::db_connect($schemaname,$username,$password,$host,$port);

    $host = $defaulthost if (not $host);
    $port = $defaultport if (not $port);
    $schemaname = $defaultschemaname if (not $schemaname);
    $username = $defaultusername if (not $username);
    $password = $defaultpassword if (not $password);

    $self->{host} = $host;
    $self->{port} = $port;
    $self->{schemaname} = $schemaname;
    $self->{username} = $username;
    $self->{password} = $password;

    if (not contains($schemaname,$self->getdatabases(),0)) {
        $self->_createdatabase($schemaname);
    }

    dbdebug($self,'connecting',$logger);

    my $dbh = DBI->connect(
        'dbi:Pg:database=' . $schemaname . ';host=' . $host . ';port=' . $port,$username,$password,
        {
            PrintError      => 0,
            RaiseError      => 0,
            AutoCommit      => 1,
            #AutoCommit      => 0,
        }
    ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),$logger);

    $dbh->{InactiveDestroy} = 1;

    #$dbh->{LongReadLen} = $LongReadLen;
    #$dbh->{LongTruncOk} = $LongTruncOk;

    $self->{dbh} = $dbh;

    $self->db_do('SET CLIENT_ENCODING TO ?',$client_encoding);

    if (length($transaction_isolation_level) > 0) {
        $self->db_do('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ' . $transaction_isolation_level);
    }

    #http://stackoverflow.com/questions/2082686/how-do-i-cast-a-string-to-integer-and-have-0-in-case-of-error-in-the-cast-with-p

    if ($enable_numeric_sorting) {
        eval {
            $self->db_do("CREATE OR REPLACE FUNCTION try_to_number (v_input text) RETURNS NUMERIC AS\n" .
            '$$' . "\n" .
            "DECLARE v_value NUMERIC DEFAULT NULL;\n" .
            "BEGIN\n" .
            "    BEGIN\n" .
            "        v_value := v_input::NUMERIC;\n" .
            "    EXCEPTION WHEN OTHERS THEN\n" .
            "        RAISE NOTICE 'Invalid integer value: \"%\".  Returning NULL.', v_input;\n" .
            "        RETURN NULL;\n" .
            "    END;\n" .
            "    RETURN v_value;\n" .
            "END;\n" .
            '$$' . "\n" .
            "LANGUAGE 'plpgsql';\n");
        };
        if ($@) {
            dbwarn($self,'numeric sorting not supported',$logger);
        }
    } else {
        dbdebug($self,'numeric sorting not enabled',$logger);
    }

    dbinfo($self,'connected',$logger);

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('VACUUM FULL ' . $self->tableidentifier($tablename));

}

sub _db_disconnect {

    my $self = shift;
    ##$self->db_finish();
    #$self->SUPER::db_finish();
    #
    #if (defined $self->{dbh}) {
    #    cleartableinfo($self);
    #    mysqldbinfo($self,'mysql db disconnecting',$logger);
    #    $self->{dbh}->disconnect() or mysqldberror($self,'error disconnecting from mysql db',$logger);
    #    $self->{dbh} = undef;
    #
    #    mysqldbinfo($self,'mysql db disconnected',$logger);
    #
    #}

    $self->SUPER::_db_disconnect();

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;

    return $self->db_get_col('SELECT attname FROM pg_attribute WHERE  attrelid = \'' . $tablename . '\'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum');

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;

    return $self->db_get_col('SELECT pg_attribute.attname FROM pg_index, pg_class, pg_attribute WHERE pg_class.oid = \'' . $tablename . '\'::regclass AND indrelid = pg_class.oid AND pg_attribute.attrelid = pg_class.oid AND pg_attribute.attnum = any(pg_index.indkey) AND indisprimary');

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
            my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
            #if (not arrayeq($indexcols,$keycols,1)) {
                #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                $indexname = lc($index_tablename) . '_' . $indexname;
                $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $temp_tablename . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
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
            my $statement = 'ALTER TABLE ' . $self->tableidentifier($tablename) . ' ADD PRIMARY KEY (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$keycols) . ')';
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
                    my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
                    if (not arrayeq($indexcols,$keycols,1)) {
                        #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                        $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $self->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
                        indexcreated($self,$tablename,$indexname,$logger);
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

            my @fieldspecs = ();
            foreach my $fieldname (@$fieldnames) {
                my $fieldnamespec = $self->columnidentifier($fieldname) . ' TEXT';
                if (not $defer_indexes and contains($fieldname,$keycols,1)) {
                    $fieldnamespec .= ' PRIMARY KEY';
                }
                push @fieldspecs,$fieldnamespec;
            }
            $statement .= join(', ',@fieldspecs) . ')';
            $self->db_do($statement);
            texttablecreated($self,$tablename,$logger);

            if (not $defer_indexes and defined $indexes and ref $indexes eq 'HASH' and scalar keys %$indexes > 0) {
                foreach my $indexname (keys %$indexes) {
                    my $indexcols = $self->_extract_indexcols($indexes->{$indexname});
                    if (not arrayeq($indexcols,$keycols,1)) {
                        #$statement .= ', INDEX ' . $indexname . ' (' . join(', ',@{$indexes->{$indexname}}) . ')';
                        $self->db_do('CREATE INDEX ' . $indexname . ' ON ' . $self->tableidentifier($tablename) . ' (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$indexcols) . ')');
                        indexcreated($self,$tablename,$indexname,$logger);
                    }
                }
            }

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
    tabletruncated($self,$tablename,$logger);

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    return $self->db_get_value('SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE tablename = ?',$tablename);
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