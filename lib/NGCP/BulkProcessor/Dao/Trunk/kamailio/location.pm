package NGCP::BulkProcessor::Dao::Trunk::kamailio::location;
use strict;

## no critic

#use threads::shared qw();

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

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    insert_row
    countby_usernamedomain
    next_ruid
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

#/*! call-id used for ul_add and ul_rm_contact */
#static str mi_ul_cid = str_init("dfjrewr12386fd6-343@Kamailio.mi");
#/*! user agent used for ul_add */
#static str mi_ul_ua  = str_init("Kamailio MI Server");

my $default_expires = 0; #4294967295
my $default_path = '<sip:127.0.0.1:5060;lr>';
my $default_q = 1.0;
my $default_cseq = 1;
my $default_callid = 'dfjrewr12386fd6-343@Kamailio.mi';
my $default_useragent = 'SIP Router MI Server'; #'Kamailio MI Server';
#\kamailio-master\src\lib\srutils\sruid.c
my $ruid_time = time();
my $ruid_counter = 0;
my $ruid_format = 'ulcx-%x-%x-%x';
my $partition_counter = 0;
my $max_partitions = undef; #>30...;

sub next_ruid {
    return sprintf($ruid_format,$ruid_time,threadid(),$ruid_counter++);
}

sub _get_partition {
    my $partition = $partition_counter + threadid();
    $partition_counter++;
    if (defined $max_partitions and $max_partitions > 0) {
        return $partition % $max_partitions;
    }
    return $partition;
}

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
            $expires,
            $ruid) = @params{qw/
                username
                domain
                contact
                q
                expires
                ruid
            /};

        $expires //= $default_expires;
        $q //= $default_q;
        $ruid //= next_ruid();
        my $partition = _get_partition();
        my $path = $default_path;
        my $cseq = $default_cseq;
        my $callid = $default_callid;
        my $useragent = $default_useragent;

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('username') . ', ' .
                $db->columnidentifier('domain') . ', ' .
                $db->columnidentifier('contact') . ', ' .
                $db->columnidentifier('path') . ', ' .
                $db->columnidentifier('q') . ', ' .
                $db->columnidentifier('last_modified') . ', ' .
                $db->columnidentifier('expires') . ', ' .
                $db->columnidentifier('cseq') . ', ' .
                $db->columnidentifier('callid') . ', ' .
                $db->columnidentifier('user_agent') . ', ' .
                $db->columnidentifier('partition') . ', ' .
                $db->columnidentifier('ruid') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                'FROM_UNIXTIME(0), ' .
                'FROM_UNIXTIME(?), ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?, ' .
                '?)',
                $username,
                $domain,
                $contact,
                $path,
                $q,
                $expires,
                $cseq,
                $callid,
                $useragent,
                $partition,
                $ruid,
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
