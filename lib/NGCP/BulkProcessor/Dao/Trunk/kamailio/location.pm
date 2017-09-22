package NGCP::BulkProcessor::Dao::Trunk::kamailio::location;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    insert_row
    countby_usernamedomain
);

my $tablename = 'location';
my $get_db = \&get_kamailio_db;

my $expected_fieldnames = [
  'id',
  'username',
  'domain',
  'contact',
  'received',
  'path',
  'expires',
  'q',
  'callid',
  'cseq',
  'last_modified',
  'flags',
  'cflags',
  'user_agent',
  'socket',
  'methods',
  'ruid',
  'reg_id',
  'instance',
  'server_id',
  'connection_id',
  'keepalive',
  'partition',

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

sub countby_usernamedomain {

    my ($username,$domain) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if (defined $username) {
        push(@terms,'username = ?');
        push(@params,$username);
    }
    if (defined $domain) {
        push(@terms,'domain = ?');
        push(@params,$domain);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub insert_row {

    my $db = &$get_db();
    my $xa_db = shift // $db;
    if ('HASH' eq ref $_[0]) {
        my ($data,$insert_ignore) = @_;
        check_table();
        if (insert_record($db,$xa_db,__PACKAGE__,$data,$insert_ignore,$insert_unique_fields)) {
            return $xa_db->db_last_insert_id();
        }
    } else {
        my %params = @_;
        my ($username,
            $domain,
            $contact,
            $q,
            $expires) = @params{qw/
                username
                domain
                contact
                q
                expires
            /};

        $expires //= 4294967295;
        $q //= 1.0;

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('username') . ', ' .
                $db->columnidentifier('domain') . ', ' .
                $db->columnidentifier('contact') . ', ' .
                $db->columnidentifier('path') . ', ' .
                $db->columnidentifier('q') . ', ' .
                $db->columnidentifier('expires') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '"<sip:127.0.0.1:5060;lr>", ' .
                '?, ' .
                '?)',
                $username,
                $domain,
                $contact,
                $q,
                $expires,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

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
