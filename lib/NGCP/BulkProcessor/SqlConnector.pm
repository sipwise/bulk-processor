package NGCP::BulkProcessor::SqlConnector;
use strict;

## no critic

use threads;
use threads::shared;

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
    $is_perl_debug
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    dbdebug
    dbinfo);
use NGCP::BulkProcessor::LogError qw(
    dberror
    dbwarn
    notimplementederror
    sortconfigerror);

use DBI;

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Array qw(arrayeq);
use NGCP::BulkProcessor::RandomString qw(createtmpstring);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(get_tableidentifier);

#my $logger = getlogger(__PACKAGE__);

my $log_db_operations = 0;

my $temptable_randomstringlength = 4;

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my $instanceid = shift;
    my $cluster = shift;

    $self->{drh} = undef;
    $self->{dbh} = undef;

    $self->{instanceid} = $instanceid;
    $self->{tid} = threadid();

    $self->{sth} = undef;
    $self->{query} = undef;
    $self->{params} = undef;

    $self->{temp_tables} = [];

    $self->{cluster} = $cluster;

    return $self;

}

sub _gettemptablename {
    my $self = shift;
    my $temp_tablename = 'TMP_TBL_' . $self->{tid} . '_';
    if (length($self->{instanceid}) > 0) {
        $temp_tablename .= $self->{instanceid} . '_';
    }
    $temp_tablename .= createtmpstring($temptable_randomstringlength); #$self->{temp_table_count};
    return $temp_tablename;
}

sub instanceidentifier {
    my $self = shift;

    $self->{instanceid} = shift if @_;
    return $self->{instanceid};

}

sub cluster {
        my $self = shift;
        $self->{cluster} = shift if @_;
        return $self->{cluster};
}

sub _connectidentifier {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub connectidentifier {
    my $self = shift;
    my $cluster = $self->{cluster};
    if (defined $cluster) {
        return $cluster->{name};
    } else {
        $self->_connectidentifier();
    }
}

sub tableidentifier {

    my $self = shift;
    my $tablename = shift;
    my (@params) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub columnidentifier {

    my $self = shift;
    my $columnname = shift;
    my (@params) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub get_tableidentifier {

    my ($tablename,@params) = @_;

    notimplementederror(__PACKAGE__ . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub getsafetablename {

    # make a table name (identifier) string save for use within create table statements
    # of this rdbms connector.
    my $self = shift;
    my ($tableidentifier) = @_; #shift;
    $tableidentifier =~ s/[^0-9a-z_]/_/gi;
    return $tableidentifier;

}

sub _extract_indexcols {

    my $self = shift;
    my $indexcols = shift;
    if (defined $indexcols and ref $indexcols eq 'ARRAY') {
        my @blankcols = map { local $_ = $_; s/\s*\(\d+\).*$//g; $_; } @$indexcols;
        return \@blankcols;
    } else {
        return [];
    }

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

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
        $statement .= ' LIMIT ' . $offset . ', ' . $limit;
    }
    return $statement;

}

sub insert_ignore_phrase {

    my $self = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub _force_numeric_column {
    my $self = shift;
    my $column = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
}

sub _orderby_columns {

    my $self = shift;
    my $sortingconfigurations = shift;

    my @orderby = ();
    if (defined $sortingconfigurations) {
        foreach my $sc (@$sortingconfigurations) {
            if (defined $sc and ref $sc eq 'HASH') {
                my $columnname = ((exists $sc->{memberchain}) ? $sc->{memberchain} : $sc->{column});
                if (ref $columnname eq 'ARRAY') {
                    $columnname = $columnname->[0];
                }
                if (length($columnname) > 0) {
                    $columnname = $self->columnidentifier($columnname);
                    my $orderby_column;
                    if ($sc->{numeric}) {
                        $orderby_column = $self->_force_numeric_column($columnname);
                    } else {
                        $orderby_column = $columnname;
                    }
                    if (!defined $sc->{dir} or $sc->{dir} > 0) {
                        $orderby_column .= ' ASC';
                    } else {
                        $orderby_column .= ' DESC';
                    }
                    push(@orderby,$orderby_column);
                } else {
                    sortconfigerror(undef,'sort column required',getlogger(__PACKAGE__));
                }
            } else {
                sortconfigerror(undef,'invalid sorting configuration',getlogger(__PACKAGE__));
            }
        }
    }
    return join(', ',@orderby);

}

sub getdatabases {

    my $self = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return [];

}

sub _createdatabase {

    my $self = shift;
    my ($databasename) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return [];

}

sub db_connect {

    my $self = shift;

    my (@params) = @_;

    if (defined $self->{dbh}) {
        $self->_db_disconnect();
    }

    # child class will do the connect stuff...

}

sub db_disconnect {
    my $self = shift;
    #my $tid = threadid();
    my $cluster = $self->{cluster};
    if (defined $cluster) {
        dbdebug($self,'disconnecting database cluster ' . $cluster->{name},getlogger(__PACKAGE__));
        foreach my $node (@{$cluster->{nodes}}) {
            if ($node->{active}) {
                my $node_db = &{$node->{get_db}}($self->{instanceid},0);
                $node_db->_db_disconnect();
            }
        }
        #$cluster->{scheduling_vars} = {};
    } else {
        $self->_db_disconnect();
    }
}

sub _db_disconnect {

    my $self = shift;

    # since this is also called from DESTROY, no die() here!

    $self->db_finish();

    if (defined $self->{dbh}) {

        #cleartableinfo($self);
        #dbdebug($self,'disconnecting' . ((defined $self->{cluster}) ? ' ' . $self->_connectidentifier() : ''),getlogger(__PACKAGE__));
        dbdebug($self,'disconnecting',getlogger(__PACKAGE__));

            foreach my $temp_tablename (@{$self->{temp_tables}}) {
                #if ($self->table_exists($temp_tablename)) {
                    $self->drop_table($temp_tablename);
                #}
            }
            $self->{temp_tables} = [];

        $self->{dbh}->disconnect() or dbwarn($self,'error disconnecting: ' . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
        $self->{dbh} = undef;

        dbinfo($self,'disconnected',getlogger(__PACKAGE__));
        #dbinfo($self,((defined $self->{cluster}) ? $self->_connectidentifier() . ' ' : '') . 'disconnected',getlogger(__PACKAGE__));

    }

    # further disconect code follows in child classes....

}

sub is_connected {

    my $self = shift;
    return (defined $self->{dbh});

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return [];

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return [];

}

sub create_temptable {

    my $self = shift;
    my $select_stmt = shift;
    my $indexes = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return '';

}

sub create_primarykey {

    my $self = shift;
    my ($tablename,$keycols,$fieldnames) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return 0;

}
sub create_indexes {

    my $self = shift;
    my ($tablename,$indexes,$keycols) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return 0;

}

sub create_texttable {

    my $self = shift;
    my ($tablename,$fieldnames,$keycols,$indexes,$truncate,$defer_indexes) = @_;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return 0;

}

sub truncate_table {

    my $self = shift;
    my $tablename = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return 0;

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub _prepare_error {

    my $self = shift;
    my $query = shift;
    dberror($self,"failed to prepare:\n" . $query . "\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));

}

sub _execute_error {

    my $self = shift;
    my $query = shift;
    my $sth = shift;
    my $errstr;
    if (defined $sth) {
        $errstr = $sth->errstr();
    } else {
        $errstr = $self->{dbh}->errstr();
    }
    dberror($self,"failed to execute:\n" . $query . "\nparameters:\n". join(', ', @_) . "\nDBI error:\n" . $errstr,getlogger(__PACKAGE__));

}
sub _fetch_error {

    my $self = shift;
    my $query = shift;
    my $sth = shift;
    my $operation = shift;
    my $index = shift;
    my $errstr;
    if (defined $sth) {
        $errstr = $sth->errstr();
    } else {
        $errstr = $self->{dbh}->errstr();
    }
    dberror($self,'failed with ' . $operation . ":\n" . $query . "\n" . ((defined $index) ? 'column index: ' . $index . "\n" : '') . "parameters:\n". join(', ', @_) . "\nDBI error:\n" . $errstr,getlogger(__PACKAGE__));

}

# "The data type is 'sticky' in that bind values passed to execute() are bound with
# the data type specified by earlier bind_param() calls, if any."
sub _bind_params {

    my $self = shift;
    my $sth = shift;
    my @params = ();
    my $p_num = 1;
    foreach my $param (@_) {
        if (defined $param and 'HASH' eq $param) {
            push(@params, delete $param->{value});
            $sth->bind_param($p_num, undef, $param);
        } else {
            push(@params,$param);
        }
        $p_num++;
    }
    return @params;

}

#sub db_autocommit {
#
#    my $self = shift;
#    if (defined $self->{dbh}) {
#        if (@_) {
#            my ($autocommit) = @_;
#            $autocommit = ($autocommit ? 1 : 0);
#            dbdebug($self,'set AutoCommit ' . $self->{dbh}->{AutoCommit} . ' -> ' . $autocommit,getlogger(__PACKAGE__));
#            $self->{dbh}->{AutoCommit} = $autocommit;
#        }
#        return $self->{dbh}->{AutoCommit};
#    }
#    return undef;
#
#}

# This method executes a SQL query that doesn't return any data. The
# query may contain placeholders, that will be replaced by the elements
# in @params during execute(). The method will die if any error occurs
# and return whatever DBI's execute() returned.
sub db_do {

    my $self = shift;
    my $query = shift;

    my $result = 0;

    if (defined $self->{dbh}) {
        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_do: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $result = $sth->execute(@params);
        if (defined $result) {
            if ('0E0' eq $result) {
                return 0;
            }
        } else {
            $self->_execute_error($query,$sth,@params);
        }
    }

    return $result;

}

# get the first value of the first row of data that is returned from the
# database. Returns undef if no data is found.
sub db_get_value {

    my $self = shift;
    my $query = shift;

    my $row = undef;

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_value: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $sth->execute(@params) or $self->_execute_error($query,$sth,@params);

        $row = $sth->fetchrow_arrayref();
        $self->_fetch_error($query,$sth,'fetchrow_arrayref',undef,@params) if !defined $row and $sth->err();
        $sth->finish();

    }

    return ((defined $row) ? $$row[0] : undef);

}

# get a reference to the first row of data that is returned from the database.
# (I.e. whatever is returned by DBI's fetchrow_hashref().)
sub db_get_row {

    my $self = shift;
    my $query = shift;

    my $row = [];

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_row: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $sth->execute(@params) or $self->_execute_error($query,$sth,@params);

        $row = $sth->fetchrow_hashref();
        $self->_fetch_error($query,$sth,'fetchrow_hashref',undef,@params) if !defined $row and $sth->err();
        $sth->finish();

    }

    return $row;

}

# get a reference to an array containing the first value of every data row that
# is returned from the database like DBI's selectcol_arrayref() does.
sub db_get_col {

    my $self = shift;
    my $query = shift;

    my $col = [];

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_col: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;

        $col = $self->{dbh}->selectcol_arrayref($sth, undef, @params);
        #die "Failed to selectcol_arrayref:\n$query\nDBI error:". $sth->errstr() if !defined $col and $sth->err();
        $self->_fetch_error($query,$sth,'selectcol_arrayref',undef,@params) if !defined $col and $sth->err();
        $sth->finish();

    }

    return $col;

}

# get all data that is returned from the database. (I.e. a reference to an
# array containing entries returned by DBI's fetchrow_hashref().)
sub db_get_all_arrayref {

    my $self = shift;
    my $query = shift;

    my @rows = ();

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_all_arrayref: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $sth->execute(@params) or $self->_execute_error($query,$sth,@params);

        while (my $row = $sth->fetchrow_hashref()) {
            $self->_fetch_error($query,$sth,'fetchrow_hashref',undef,@params) if $sth->err();
            push @rows, $row;
        }
        $sth->finish();

    }

    return \@rows;

}

# get a reference to a hash containing a hashreference for each row, like DBI's
# fetchall_hashref() does.
sub db_get_all_hashref {

    my $self = shift;
    my $query = shift;
    my $index = shift;

    my $result = {};

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_all_hashref: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $sth->execute(@params) or $self->_execute_error($query,$sth,@params);

        $result = $sth->fetchall_hashref($index);
        $self->_fetch_error($query,$sth,'fetchall_hashref',$index,@params) if $sth->err();
        $sth->finish();

    }

    return $result;

}

# get a reference to a hash that is composed of the key_column as keys and the
# value_column as values.
sub db_get_mapref {

    my $self = shift;
    my $query = shift;
    my $index = shift;
    my $value = shift;

    my $result = {};

    if (defined $self->{dbh}) {

        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($sth,@_);
        dbdebug($self,'db_get_mapref: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
        $sth->execute(@_) or $self->_execute_error($query,$sth,@params);

        my $rows = $sth->fetchall_hashref($index);
        #die "Failed to fetchall_hashref:\n$query\nDBI error:". $sth->errstr() if $sth->err();
        $self->_fetch_error($query,$sth,'fetchall_hashref',$index,@params) if $sth->err();

        foreach my $key (keys %$rows) {
            $result->{$key} = $$rows{$key}{$value};
        }
        $sth->finish();

        return $result;

    }

    return $result;

}

sub db_begin {

    my $self = shift;
    if (defined $self->{dbh}) {
        dbdebug($self,'db_begin',getlogger(__PACKAGE__));
        $self->{dbh}->begin_work() or dberror($self, "failed with begin_transaction \nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));

        if ($self->{dbh}->{AutoCommit}) {
            dbwarn($self,'autocommit was not disabled',getlogger(__PACKAGE__));
        }

    }

}

sub db_commit {

    my $self = shift;
    if (defined $self->{dbh}) {
        dbdebug($self,'db_commit',getlogger(__PACKAGE__));
        if ($is_perl_debug) { #https://rt.cpan.org/Public/Bug/Display.html?id=102791
            # no context:
            $self->{dbh}->commit();
            if ($DBI::err) {
                dberror($self, "failed to commit changes\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
            }
        } else {
            #my @wa =
            $self->{dbh}->commit() or dberror($self, "failed to commit changes\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
        }
    }

}

sub db_rollback {

    my $self = shift;
    my ($log) = @_;
    if (defined $self->{dbh}) {
        dbdebug($self,'db_rollback',getlogger(__PACKAGE__));
        if ($is_perl_debug) {
            $self->{dbh}->rollback();
            if ($DBI::err) {
                dberror($self, "failed to rollback changes\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
            }
        } else {
            $self->{dbh}->rollback() or dberror($self, "failed to rollback changes\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
        }
        dbinfo($self,'transaction rolled back',getlogger(__PACKAGE__)) if $log;
    }

}

sub db_quote {

    my $self = shift;
    my $value = shift;

    my $result = $value;

    if (defined $self->{dbh}) {
        $result = $self->{dbh}->quote($value) or dberror($self, "failed to quote value\nDBI error:\n" . $self->{dbh}->errstr(),getlogger(__PACKAGE__));
    }
    return $result;

}

sub db_last_insert_id {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub DESTROY {

    my $self = shift;

    # perl threads works like a fork, each thread owns a shalow? copy
    # of the entire current context, at the moment it starts.
    # due to this, if the thread is finished, perl gc will invoke destructors
    # on the thread's scope elements, that potentially contains connectors from
    # the main tread. it will actually attempt destroy them (disconect, etc.)
    # this is a problem with destructors that change object state like this one
    #
    # to avoid this, we perform destruction tasks only if the destructing tid
    # is the same as the creating one:

    if ($self->{tid} == threadid()) {
        $self->_db_disconnect();
        delete $self->{drh};
        dbdebug($self,(ref $self) . ' connector destroyed',getlogger(__PACKAGE__));
    #} else {
    #    print "NOT destroyed\n";
    }

}

sub lock_tables {

    my $self = shift;
    my $tablestolock = shift;

    #$self->db_begin();
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub unlock_tables {

    my $self = shift;

    #$self->db_commit();
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub db_do_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;
    my $transactional = shift;

    #notimplementederror('db_do_begin',getlogger(__PACKAGE__));

    if (defined $self->{dbh} and !defined $self->{sth}) { # and length($tablename) > 0) {

        dbdebug($self,'db_do_begin: ' . $query,getlogger(__PACKAGE__));
        if ($transactional) {
            #$self->lock_tables({ $tablename => 'WRITE' });
            $self->db_begin();
        }

        $self->{sth} = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        $self->{query} = $query;
        $self->{params} = [];

    }


}

sub db_do_rowblock {

    my $self = shift;
    my $rows = shift;

    #notimplementederror('db_do_rowblock',getlogger(__PACKAGE__));

    if (defined $self->{dbh} and defined $self->{sth} and defined $rows and ref $rows eq 'ARRAY') {

        #dberror($self,'test error',getlogger(__PACKAGE__));
        #mysqldbdebug($self,"db_do_rowblock\nrows:\n" . (scalar @$rows),getlogger(__PACKAGE__));
        #mysqldbdebug($self,'db_do_rowblock: ' . $self->{query} . "\nparameters:\n" . join(', ', @_),getlogger(__PACKAGE__));
        foreach my $row (@$rows) {
            my @params = $self->_bind_params($self->{sth},@$row);
            dbdebug($self,'db_do_rowblock: ' . $self->{query} . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__)) if $log_db_operations;
            $self->{sth}->execute(@params) or $self->_execute_error($self->{query},$self->{sth},@params);
            $self->{params} = \@params;
        }

    }

}

sub db_get_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;
    my $transactional = shift;

    if (defined $self->{dbh} and !defined $self->{sth}) { # and length($tablename) > 0) {

        #eval { $self->lock_tables({ $tablename => 'WRITE' }); };
        if ($transactional) {
            #$self->lock_tables({ $tablename => 'WRITE' });
            $self->db_begin();
        }

        $self->{sth} = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        my @params = $self->_bind_params($self->{sth},@_);
        dbdebug($self,'db_get_begin: ' . $query . "\nparameters:\n" . join(', ', @params),getlogger(__PACKAGE__));
        $self->{sth}->execute(@params) or $self->_execute_error($query,$self->{sth},@params);
        $self->{query} = $query;
        $self->{params} = \@params;

    }

}

sub multithreading_supported {

    my $self = shift;
    return 0;

}

sub db_get_rowblock {

    my $self = shift;
    my $max_rows = shift;

    if ($enablemultithreading) {

        #my $rows : shared = [];
        my @rows :shared = ();
        #my $rows = &share([]); # beware of '&' here!!!!
        #my $rows = shared_clone({});

        if (defined $self->{dbh} and defined $self->{sth}) {

            dbdebug($self,'db_get_rowblock: ' . $self->{query} . "\nparameters:\n" . join(', ', @{$self->{params}}),getlogger(__PACKAGE__)) if $log_db_operations;

            foreach (@{$self->{sth}->fetchall_arrayref(undef, $max_rows)}) {
                my @row : shared = @{$_};
                push @rows, \@row;
            }


            $self->_fetch_error($self->{query},$self->{sth},'db_get_rowblock',undef,@{$self->{params}}) if $self->{sth}->err();

        }

        #share(@rows);
        return \@rows;
        #return $rows;
        #return \@rows;

    } else {

        my $rows = [];

        if (defined $self->{dbh} and defined $self->{sth}) {

            dbdebug($self,'db_get_rowblock: ' . $self->{query} . "\nparameters:\n" . join(', ', @{$self->{params}}),getlogger(__PACKAGE__)) if $log_db_operations;
            $rows = $self->{sth}->fetchall_arrayref(undef, $max_rows);
            $self->_fetch_error($self->{query},$self->{sth},'db_get_rowblock',undef,@{$self->{params}}) if $self->{sth}->err();

        }

        return $rows;

    }

}

sub db_finish {

    my $self = shift;
    my $transactional = shift;
    my $rollback = shift;

    # since this is also called from DESTROY, no die() here!

    if (defined $self->{dbh} and defined $self->{sth}) {

        dbdebug($self,'db_finish',getlogger(__PACKAGE__));

        $self->{sth}->finish();
        $self->{sth} = undef;

        if ($transactional) {
            #$self->unlock_tables();
            if ($rollback) {
                $self->db_rollback(1);
            } else {
                $self->db_commit();
            }
        }

        $self->{query} = undef;
        $self->{params} = undef;

    }

}

1;
