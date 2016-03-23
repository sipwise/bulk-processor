package SqlConnectors::SQLiteDB;
use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Globals qw(
    $local_db_path
    $LongReadLen_limit);
use Logging qw(
    getlogger
    dbinfo
    dbdebug
    texttablecreated
    temptablecreated
    indexcreated
    tabletruncated
    tabledropped);
use LogError qw(
    dberror
    fieldnamesdiffer
    dbwarn
    fileerror
    filewarn);

use DBI 1.608 qw(:sql_types);
use DBD::SQLite 1.29;
use Array qw(arrayeq contains setcontains);

use Utils qw(
    tempfilename
    timestampdigits
    timestamp);

use SqlConnectors::SQLiteVarianceAggregate;

use SqlConnector;

require Exporter;
our @ISA = qw(Exporter SqlConnector);
our @EXPORT_OK = qw($staticdbfilemode
                    $timestampdbfilemode
                    $temporarydbfilemode
                    $memorydbfilemode
                    $privatedbfilemode
                    get_tableidentifier
                    cleanupdbfiles);

our $staticdbfilemode = 0; #remains on disk after shutdown
our $timestampdbfilemode = 1; #remains on disk after shutdown
our $temporarydbfilemode = 2; #cleaned on shutdown
our $memorydbfilemode = 3; #never on disk
our $privatedbfilemode = 4; #somewhere on disk, cleaned on shutdown

my $cachesize = 16384; #40000;
my $pagesize = 2048; #8192;
my $busytimeout = 20000; #msecs

my $dbextension = '.db';
my $journalpostfix = '-journal';

my $texttable_encoding = 'UTF-8'; # sqlite returns whats inserted...

$DBD::SQLite::COLLATION{no_accents} = sub {
    my ( $a, $b ) = map lc, @_;
    tr[����������������������������]
      [aaaaaacdeeeeiiiinoooooouuuuy] for $a, $b;
    $a cmp $b;
  };

my $LongReadLen = $LongReadLen_limit; #bytes
my $LongTruncOk = 0;

my $logger = getlogger(__PACKAGE__);

my $lock_do_chunk = 1;
my $lock_get_chunk = 1;

#SQLite transactions are always serializable.

sub new {

    my $class = shift;

    my $self = SqlConnector->new(@_);

    $self->{filemode} = undef;
    $self->{dbfilename} = undef;

    $self->{drh} = DBI->install_driver('SQLite');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',$logger);

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    return _get_connectidentifier($self->{filemode},$self->{dbfilename});

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

    my ($tablename,$filemode, $filename) = @_;
    my $connectionidentifier = _get_connectidentifier($filemode, $filename);
    if (defined $connectionidentifier) {
        return $connectionidentifier . '.' . $tablename;
    } else {
        return $tablename;
    }

}

sub getsafetablename {

    my $self = shift;
    my $tableidentifier = shift;

    return $self->SUPER::getsafetablename($tableidentifier);

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    return 'CAST(' . $column . ' AS REAL)';
}

sub getdatabases {

    my $self = shift;

    my $rdbextension = quotemeta($dbextension);
    my $ucrdbextension = quotemeta(uc($dbextension));
    #my $rjournalpostfix = quotemeta($journalpostfix);
    local *DBDIR;
    if (not opendir(DBDIR, $local_db_path)) {
        fileerror('cannot opendir ' . $local_db_path . ': ' . $!,$logger);
        return [];
    }
    my @files = grep { /($rdbextension|$ucrdbextension)$/ && -f $local_db_path . $_ } readdir(DBDIR);
    closedir DBDIR;
    my @databases = ();
    foreach my $file (@files) {
        #print $file;
        my $databasename = $file;
        $databasename =~ s/($rdbextension|$ucrdbextension)$//g;
        push @databases,$databasename;
    }
    return \@databases;

}

sub _createdatabase {

    my $self = shift;
    my ($filename) = @_;
    my $dbfilename = _getdbfilename($self->{filemode},$filename);

    if ($self->_is_filebased() and not -e $dbfilename) {
        my $dbh = DBI->connect(
            'dbi:SQLite:dbname=' . $dbfilename, '', '',
            {
                PrintError      => 0,
                RaiseError      => 0,
                #sqlite_unicode  => 1, latin 1 chars
                #AutoCommit      => 0,
            }
        ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),$logger);
        $dbh->disconnect() or dbwarn($self,'error disconnecting: ' . $dbh->errstr(),$logger);
        dbinfo($self,'database \'' . $dbfilename . '\' created',$logger);
    }

    return $dbfilename;

}

sub db_connect {

    my $self = shift;
    my ($filemode, $filename) = @_;

    $self->SUPER::db_connect($filemode, $filename);

    #if (defined $self->{dbh}) {
    #    $self->db_disconnect();
    #}

    $self->{filemode} = $filemode;
    $self->{dbfilename} = $self->_createdatabase($filename);


    my $dbh = DBI->connect(
        'dbi:SQLite:dbname=' . $self->{dbfilename}, '', '',
        {
            PrintError      => 0,
            RaiseError      => 0,
            #sqlite_unicode  => 1, latin 1 chars
            #AutoCommit      => 0,
        }
    ) or dberror($self,'error connecting: ' . $self->{drh}->errstr(),$logger);
    #or sqlitedberror($dbfilename,'error connecting to sqlite db',$logger);

    $dbh->{InactiveDestroy} = 1;

    $dbh->{LongReadLen} = $LongReadLen;
    $dbh->{LongTruncOk} = $LongTruncOk;

    $dbh->{AutoCommit} = 1;
    # we use a mysql style
    $dbh->sqlite_create_function('now', 0, \&timestamp );
    $dbh->sqlite_create_function('concat', 2, \&_concat );
    #$dbh->sqlite_create_function(float_equal ??
    $dbh->sqlite_create_aggregate( 'variance', 1, 'SQLiteVarianceAggregate' );

    $dbh->sqlite_busy_timeout($busytimeout);

    $self->{dbh} = $dbh;

    #SQLite transactions are always serializable.

    $self->db_do('PRAGMA foreign_keys = OFF');
    #$self->db_do('PRAGMA default_synchronous = OFF');
    $self->db_do('PRAGMA synchronous = OFF');
    $self->db_do('PRAGMA page_size = ' . $pagesize);
    $self->db_do('PRAGMA cache_size = ' . $cachesize);
    #$self->db_do('PRAGMA encoding = "UTF-8"'); # only new databases!
    $self->db_do('PRAGMA encoding = "' . $texttable_encoding . '"'); # only new databases!
    #PRAGMA locking_mode = NORMAL ... by default
    #$self->db_do('PRAGMA auto_vacuum = INCREMENTAL');

    dbinfo($self,'connected',$logger);

}

sub _concat {

    return $_[0] . $_[1];

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

    $self->db_finish();

    if (defined $self->{dbh}) {
        if ($self->{filemode} == $staticdbfilemode or $self->{filemode} == $timestampdbfilemode) {
            $self->db_do('VACUUM'); # or sqlitedberror($self,"failed to VACUUM\nDBI error:\n" . $self->{dbh}->errstr(),$logger);
            dbinfo($self,'VACUUMed',$logger);
        }
    }

}

sub _db_disconnect {

    my $self = shift;

    $self->SUPER::_db_disconnect();

    if ($self->{filemode} == $temporarydbfilemode and defined $self->{dbfilename} and -e $self->{dbfilename}) {
        if ((unlink $self->{dbfilename}) > 0) {
            dbinfo($self,'db file removed',$logger);
        } else {
            dbwarn($self,'cannot remove db file: ' . $!,$logger);
        }
        my $journalfilename = $self->{dbfilename} . '-journal';
        if (-e $journalfilename) {
            if ((unlink $journalfilename) > 0) {
                dbinfo($self,'journal file removed',$logger);
            } else {
                dbwarn($self,'cannot remove journal file: ' . $!,$logger);
            }
        }
    }

}


sub cleanupdbfiles {

    my (@remainingdbfilenames) = @_;
    my $rdbextension = quotemeta($dbextension);
    my $ucrdbextension = quotemeta(uc($dbextension));
    my $rjournalpostfix = quotemeta($journalpostfix);
    local *DBDIR;
    if (not opendir(DBDIR, $local_db_path)) {
        fileerror('cannot opendir ' . $local_db_path . ': ' . $!,$logger);
        return;
    }
    my @files = grep { /($rdbextension|$ucrdbextension)($rjournalpostfix)?$/ && -f $local_db_path . $_ } readdir(DBDIR);
    closedir DBDIR;
    my @remainingdbfiles = ();
    foreach my $filename (@remainingdbfilenames) {
        push @remainingdbfiles,$local_db_path . $filename . $dbextension;
        push @remainingdbfiles,$local_db_path . $filename . $dbextension . $journalpostfix;
        push @remainingdbfiles,$local_db_path . uc($filename . $dbextension) . $journalpostfix;
    }
    foreach my $file (@files) {
        #print $file;
        my $filepath = $local_db_path . $file;
        if (not contains($filepath,\@remainingdbfiles)) {
            if ((unlink $filepath) == 0) {
                filewarn('cannot remove ' . $filepath . ': ' . $!,$logger);
            }
        }
    }

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;
    my @fieldnames = keys %{$self->db_get_all_hashref('PRAGMA table_info(' . $tablename . ')','name')};
    return \@fieldnames;

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;
    #return $self->db_get_col('SHOW FIELDS FROM ' . $tablename);
    my $fieldinfo = $self->db_get_all_hashref('PRAGMA table_info(' . $tablename . ')','name');
    my @keycols = ();
    foreach my $fieldname (keys %$fieldinfo) {
        if ($fieldinfo->{$fieldname}->{'pk'}) {
            push @keycols,$fieldname;
        }
    }
    return \@keycols;

}

sub create_primarykey {

    my $self = shift;
    my ($tablename,$keycols,$fieldnames) = @_;

    #not supported by sqlite

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

sub create_temptable {

    my $self = shift;
    my $select_stmt = shift;
    my $indexes = shift;

    my $index_tablename = $self->_gettemptablename();
    my $temp_tablename = $self->tableidentifier($index_tablename);

    $self->db_do('CREATE TEMPORARY TABLE ' . $temp_tablename . ' AS ' . $select_stmt);
    #push(@{$self->{temp_tables}},$temp_tablename);
    temptablecreated($self,$index_tablename,$logger);

    #$self->{temp_table_count} += 1;

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

sub create_texttable {

    my $self = shift;
    my ($tablename,$fieldnames,$keycols,$indexes,$truncate,$defer_indexes) = @_;
    #my ($tableidentifier,$fieldnames,$keycols,$indexes,$truncate) = @_;

    #my $tablename = $self->getsafetablename($tableidentifier);

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        my $created = 0;
        if ($self->table_exists($tablename) == 0) {
            my $statement = 'CREATE TABLE ' . $self->tableidentifier($tablename) . ' (';
            $statement .= join(' TEXT, ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$fieldnames) . ' TEXT'; # sqlite_unicode off... outcoming strings not marked utf8
            #$statement .= join(' BLOB, ',@$fieldnames) . ' BLOB'; #to maintain source char encoding when inserting?
            #if (not $defer_indexes and defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
            if (defined $keycols and ref $keycols eq 'ARRAY' and scalar @$keycols > 0 and setcontains($keycols,$fieldnames,1)) {
                $statement .= ', PRIMARY KEY (' . join(', ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$keycols) . ')';
            }
            $statement .= ')';

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

    $self->db_do('DELETE FROM ' . $self->tableidentifier($tablename));
    #$self->db_do('VACUUM');
    tabletruncated($self,$tablename,$logger);

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    return $self->db_get_value('SELECT COUNT(*) FROM sqlite_master WHERE type = \'table\' AND name = ?',$tablename);

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    if ($self->table_exists($tablename) > 0) {
        $self->db_do('DROP TABLE ' . $self->tableidentifier($tablename));

        #my $indexes = $self->db_get_col('SELECT name FROM sqlite_master WHERE type = \'index\' AND tbl_name = ?',$tablename);
        #foreach my $indexname (@$indexes) {
        #    $self->db_do('DROP INDEX IF EXISTS ' . $indexname);
        #}


        #$self->db_do('VACUUM');
        tabledropped($self,$tablename,$logger);
        return 1;
    }
    return 0;

}

sub _get_connectidentifier {

    my ($filemode, $filename) = @_;
    if ($filemode == $staticdbfilemode and defined $filename) {
        return $filename;
    } elsif ($filemode == $timestampdbfilemode) {
        return $filename;
    } elsif ($filemode == $temporarydbfilemode) {
        return $filename;
    } elsif ($filemode == $memorydbfilemode) {
        return '<InMemoryDB>';
    } elsif ($filemode == $privatedbfilemode) {
        return '<PrivateDB>';
    } else {
        return undef;
    }

}

sub _getdbfilename {

    my ($filemode,$filename) = @_;
    if ($filemode == $staticdbfilemode and defined $filename) {
        return $local_db_path . $filename . $dbextension;
    } elsif ($filemode == $timestampdbfilemode) {
        return $local_db_path . timestampdigits() . $dbextension;
    } elsif ($filemode == $temporarydbfilemode) {
        return tempfilename('XXXX',$local_db_path,$dbextension);
    } elsif ($filemode == $memorydbfilemode) {
        return ':memory:';
    } elsif ($filemode == $privatedbfilemode) {
        return '';
    }

}

sub _is_filebased {

    my $self = shift;
    if ($self->{filemode} == $staticdbfilemode or $self->{filemode} == $timestampdbfilemode or $self->{filemode} == $temporarydbfilemode) {
        return 1;
    } else {
        return 0;
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

1;