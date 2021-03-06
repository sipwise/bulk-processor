package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_sound_handles;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
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

    findby_namegroup

);

my $tablename = 'voip_sound_handles';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'name',
  'group_id',
];

my $indexes = {};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}


sub findby_namegroup {

    my ($name,$group,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT h.*,g.name as group_name FROM ' . $table . ' h JOIN provisioning.voip_sound_groups g ON h.group_id = g.id';
    #' WHERE h.name = ? AND g.name = ?';
    my @params = ();
    my @terms = ();
    if (defined $name) {
        push(@terms,'h.name = ?');
        push(@params,$name);
    }
    if (defined $group) {
        push(@terms,'g.name = ?');
        push(@params,$group);
    }
    $stmt .= ' WHERE ' . join(' AND ',@terms) if (scalar @terms) > 0;
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
            $record->{group} = $row->{group_name};

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
