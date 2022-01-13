package NGCP::BulkProcessor::Projects::ETL::EDR::Dao::PeriodEvents;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::ETL::EDR::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
);

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row
    insert_stmt
    transfer_table
);

use NGCP::BulkProcessor::SqlRecord qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    
    copy_table
);

my $tablename = 'period_events';
my $get_db = \&get_sqlite_db;

my $fieldnames;
my $expected_fieldnames = [
    'subscriber_id',
    'profile_id',
    'start_profile',
    'update_profile',
    'stop_profile',
];

my $primarykey_fieldnames = [];
my $indexes = {
    $tablename . '_suscriber_id' => [ 'subscriber_id(11)' ],
};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub create_table {

    my ($truncate) = @_;

    my $db = &$get_db();

    registertableinfo($db,__PACKAGE__,$tablename,$expected_fieldnames,$indexes,$primarykey_fieldnames);
    return create_targettable($db,__PACKAGE__,$db,__PACKAGE__,$tablename,$truncate,1,undef);

}

sub findby_domainusername {

    my ($domain,$username,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless (defined $domain and defined $username);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' . $table .
        ' WHERE ' . $db->columnidentifier('domain') . ' = ?' .
        ' AND ' . $db->columnidentifier('username') . ' = ?'
    , $domain, $username);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub copy_table {
    
    my ($get_target_db) = @_;
     
    check_table();
    #checktableinfo($get_target_db,
    #    __PACKAGE__,$tablename,
    #    get_fieldnames(1),
    #    $indexes);

    return transfer_table(
        get_db => $get_db,
        class => __PACKAGE__,
        get_target_db => $get_target_db,
        targetclass => __PACKAGE__,
        targettablename => $tablename,
    );
    
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

sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,__PACKAGE__,$insert_ignore);

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
