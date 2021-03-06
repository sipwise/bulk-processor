package NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
    rowsdeleted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_provisioning_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    delete_record
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

# required to use the constants:
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row
    delete_row

    delete_preferences

    findby_subscriberid_attributeid
    countby_subscriberid_attributeid_value
    findby_subscriberid

    $TRUE
    $FALSE
);

my $tablename = 'voip_usr_preferences';
my $get_db = \&get_provisioning_db;

my $expected_fieldnames = [
    'id',
    'subscriber_id',
    'attribute_id',
    'value',
    'modify_timestamp',
];

my $indexes = {};

my $insert_unique_fields = [];

our $TRUE = 1;
our $FALSE = undef;

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_subscriberid {

    my ($subscriber_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT v.*,a.attribute FROM ' . $table . ' v JOIN ' .
            $db->tableidentifier('voip_preferences') . ' a ON v.attribute_id = a.id WHERE ' .
            'v.subscriber_id = ?';
    my @params = ($subscriber_id);
    my $rows = $db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_subscriberid_attributeid {

    my ($xa_db,$subscriber_id,$attribute_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $attribute_id) {
        $stmt .= ' AND ' . $db->columnidentifier('attribute_id') . ' = ?';
        push(@params,$attribute_id);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub countby_subscriberid_attributeid_value {

    my ($subscriber_id,$attribute_id,$value) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    my @terms = ();
    if ($subscriber_id) {
        push(@terms,$db->columnidentifier('subscriber_id') . ' = ?');
        push(@params,$subscriber_id);
    }
    if ($attribute_id) {
        push(@terms,$db->columnidentifier('attribute_id') . ' = ?');
        push(@params,$attribute_id);
    }
    if ($value) {
        push(@terms,$db->columnidentifier('value') . ' = ?');
        push(@params,$value);
    }
    if ((scalar @terms) > 0) {
        $stmt .= ' WHERE ' . join(' AND ',@terms);
    }

    return $db->db_get_value($stmt,@params);

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub delete_row {

    my ($xa_db,$data) = @_;

    check_table();
    return delete_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub delete_preferences {

    my ($xa_db,$subscriber_id,$attribute_id,$vals) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'DELETE FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('subscriber_id') . ' = ?';
    my @params = ($subscriber_id);
    if (defined $attribute_id) {
        $stmt .= ' AND ' . $db->columnidentifier('attribute_id') . ' = ?';
        push(@params,$attribute_id);
    }
    if (defined $vals and 'HASH' eq ref $vals) {
        foreach my $in (keys %$vals) {
            my @values = (defined $vals->{$in} and 'ARRAY' eq ref $vals->{$in} ? @{$vals->{$in}} : ($vals->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('value') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    }

    my $count;
    if ($count = $xa_db->db_do($stmt,@params)) {
        rowsdeleted($db,$tablename,$count,$count,getlogger(__PACKAGE__));
        return 1;
    } else {
        rowsdeleted($db,$tablename,0,0,getlogger(__PACKAGE__));
        return 0;
    }

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
        my ($attribute_id,
            $subscriber_id,
            $value) = @params{qw/
                attribute_id
                subscriber_id
                value
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('attribute_id') . ', ' .
                $db->columnidentifier('subscriber_id') . ', ' .
                $db->columnidentifier('value') . ') VALUES (' .
                '?, ' .
                '?, ' .
                '?)',
                $attribute_id,
                $subscriber_id,
                $value,
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
            $record->{_attribute} = $row->{attribute};
            $record->load_relation($load_recursive,'attribute','NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_id',$record->{attribute_id},$load_recursive);
            $record->{_attribute} //= $record->{attribute}->{attribute} if exists $record->{attribute};
            if ($record->{_attribute}) {
                $record->load_relation($load_recursive,'allowed_ips','NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups::findby_group_id',$record->{value},$load_recursive)
                    if ($record->{_attribute} eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE
                        or $record->{_attribute} eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::MAN_ALLOWED_IPS_GRP_ATTRIBUTE);
                $record->load_relation($load_recursive,'ncos','NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_id',$record->{value},$load_recursive)
                    if ($record->{_attribute} eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::NCOS_ID_ATTRIBUTE
                        or $record->{_attribute} eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ID_ATTRIBUTE);
                $record->load_relation($load_recursive,"cf_mapping",'NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_cf_mappings::findby_id',$record->{value},$load_recursive)
                    if (grep { $record->{_attribute} eq $_; } @NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CF_ATTRIBUTES);
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

    return checktableinfo($get_db,
                   __PACKAGE__,$tablename,
                   $expected_fieldnames,
                   $indexes);

}

1;
