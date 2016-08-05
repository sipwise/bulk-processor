package NGCP::BulkProcessor::Dao::mr441::billing::lnp_providers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
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

    findby_prefix
);

my $tablename = 'lnp_providers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'name',
    'prefix',
    #'authoritative',
    #'skip_rewrite',
];

my $indexes = {};

my $insert_unique_fields = [];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($get_db,
                           $tablename,
                           $expected_fieldnames,$indexes);

    bless($self,$class);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_prefix {

    my ($prefix,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($prefix) {
        push(@terms,$db->columnidentifier('prefix') . ' = ?');
        push(@params,$prefix);
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
