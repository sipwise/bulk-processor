package NGCP::BulkProcessor::Dao::mr553::provisioning::voip_sound_sets;
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

use NGCP::BulkProcessor::Dao::mr553::provisioning::voip_sound_files qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table


);

my $tablename = 'voip_sound_sets';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
  'id',
  'reseller_id',
  'contract_id',
  'name',
  'description',
  'contract_default',

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

    return checktableinfo(shift // $get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

sub source_new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,shift,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub source_findby_resellerid {

    my ($source_dbs,$reseller_id) = @_;

    my $source_db = $source_dbs->{provisioning_db};
    check_table($source_db);
    my $db = &$source_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('reseller_id') . ' = ? and contract_id is null';
    my @params = ($reseller_id);

    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return source_buildrecords_fromrows($rows,$source_dbs);

}

sub source_buildrecords_fromrows {

    my ($rows,$source_dbs) = @_;

    my @records = (); # : shared = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        foreach my $row (@$rows) {
            $record = __PACKAGE__->source_new($source_dbs->{provisioning_db},$row);

            # transformations go here ...
            $record->{sound_files} = NGCP::BulkProcessor::Dao::mr553::provisioning::voip_sound_files::source_findby_setid($source_dbs,$record->{id});


            push @records,$record;
        }
    }

    return \@records;

}

1;
