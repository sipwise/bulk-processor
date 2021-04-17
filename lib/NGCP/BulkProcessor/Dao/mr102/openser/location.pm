package NGCP::BulkProcessor::Dao::mr102::openser::location;
use strict;

## no critic

use Locale::Recode qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger

);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_kamailio_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo

    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table

    findby_usernamedomain
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
];

my $indexes = {};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_usernamedomain {

    my ($username,$domain,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
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

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;
    
    my $recoder = Locale::Recode->new( from => 'ISO-8859-1', to => 'UTF-8' );

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...
            foreach my $field (keys %$record) {
                $record->{$field} = $recoder->recode($record->{$field}) if $record->{field};
            }            

            push @records,$record;
        }
    }

    return \@records;

}

sub gettablename {

    return $tablename;

}

sub check_table {

    return checktableinfo(shift // $get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
