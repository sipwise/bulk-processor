package NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers;
use strict;

## no critic

use NGCP::BulkProcessor::Logging qw(
    getlogger
    rowinserted
);

use NGCP::BulkProcessor::ConnectorPool qw(
    get_billing_db
);

use NGCP::BulkProcessor::SqlProcessor qw(
    checktableinfo
    insert_record
    update_record
    delete_record
    process_table
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row
    delete_row

    findby_domainid_username_states
    countby_status_resellerid
    process_records
    find_minmaxid
    find_random
    findby_contractid_states
    findby_domainid_usernames

    $TERMINATED_STATE
    $ACTIVE_STATE
);

my $tablename = 'voip_subscribers';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'uuid',
    'username',
    'domain_id',
    'status',
    'primary_number_id',
    'external_id',
    'contact_id',
];

my $indexes = {};

my $insert_unique_fields = [];

our $TERMINATED_STATE = 'terminated';
our $ACTIVE_STATE = 'active';

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::SqlRecord->new($class,$get_db,
                           $tablename,$expected_fieldnames,$indexes);

    copy_row($self,shift,$expected_fieldnames);

    return $self;

}

sub findby_domainid_usernames {

    my ($xa_db,$domain_id,$usernames,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('domain_id') . ' = ?';
    my @params = ($domain_id);
    if (defined $usernames and 'ARRAY' eq ref $usernames) {
        $stmt .= ' AND ' . $db->columnidentifier('username') . ' IN (' . substr(',?' x scalar @$usernames,1) . ')';
        push(@params,@$usernames);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_domainid_username_states {

    my ($xa_db,$domain_id,$username,$states,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('domain_id') . ' = ?' .
            ' AND ' . $db->columnidentifier('username') . ' = ?';
    my @params = ($domain_id,$username);
    if (defined $states and 'HASH' eq ref $states) {
        foreach my $in (keys %$states) {
            my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('status') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $states and length($states) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('status') . ' = ?';
        push(@params,$states);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_contractid_states {

    my ($xa_db,$contract_id,$states,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contract_id') . ' = ?';
    my @params = ($contract_id);
    if (defined $states and 'HASH' eq ref $states) {
        foreach my $in (keys %$states) {
            my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('status') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $states and length($states) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('status') . ' = ?';
        push(@params,$states);
    }
    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub find_minmaxid {

    my ($xa_db,$states,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my @ids = ();
    foreach my $func ('MIN','MAX') {
        my @params = ();
        my $stmt = 'SELECT ' . $func . '(r1.id) FROM ' . $table . ' AS r1';
        if ($reseller_id) {
            $stmt .= ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contracts::gettablename()) . ' AS contract ON r1.contract_id = contract.id' .
            ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contacts::gettablename()) . ' AS contact ON contract.contact_id = contact.id';
        }
        $stmt .= ' WHERE 1=1';
        if ($reseller_id) {
            if ('ARRAY' eq ref $reseller_id) {
                $stmt .= ' AND contact.reseller_id IN (' . substr(',?' x scalar @$reseller_id,1) . ')';
                push(@params,@$reseller_id);
            } else {
                $stmt .= ' AND contact.reseller_id = ?';
                push(@params,$reseller_id);
            }
        }
        if (defined $states and 'HASH' eq ref $states) {
            foreach my $in (keys %$states) {
                my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
                $stmt .= ' AND r1.status ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
                push(@params,@values);
            }
        } elsif (defined $states and length($states) > 0) {
            $stmt .= ' AND r1.status = ?';
            push(@params,$states);
        }
        push(@ids,$db->db_get_value($stmt,@params));
    }
    return @ids;

}

sub find_random {

    my ($xa_db,$excluding_id,$states,$reseller_id,$min_id,$max_id,$load_recursive) = @_;

    if (not defined $min_id or not defined $max_id) {
        ($min_id,$max_id) = find_minmaxid($xa_db,$states,$reseller_id);
    }

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    die("less than 2 subscriber to randomize") if ($max_id - $min_id) < 2;
    my $rand = $min_id + int(rand($max_id - $min_id) + 0.5);
    if (defined $excluding_id) {
        while ($rand == $excluding_id) {
            $rand = $min_id + int(rand($max_id - $min_id) + 0.5);
        }
    }
    my $stmt = 'SELECT r1.* FROM ' . $table . ' AS r1' .
      ' JOIN (SELECT ? AS id) AS r2';
    my @params = ();
    push(@params,$rand);
    if ($reseller_id) {
        $stmt .= ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contracts::gettablename()) . ' AS contract ON r1.contract_id = contract.id' .
        ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contacts::gettablename()) . ' AS contact ON contract.contact_id = contact.id';
    }
    $stmt .= ' WHERE r1.id >= r2.id'; # AND r1.id <= ?';
    #push(@params,$max_id);

    if (defined $states and 'HASH' eq ref $states) {
        foreach my $in (keys %$states) {
            my @values = (defined $states->{$in} and 'ARRAY' eq ref $states->{$in} ? @{$states->{$in}} : ($states->{$in}));
            $stmt .= ' AND r1.status ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $states and length($states) > 0) {
        $stmt .= ' AND r1.status = ?';
        push(@params,$states);
    }
    if (defined $excluding_id) {
        $stmt .= ' AND r1.id != ?';
        push(@params,$excluding_id);
    }
    if ($reseller_id) {
        if ('ARRAY' eq ref $reseller_id) {
            $stmt .= ' AND contact.reseller_id IN (' . substr(',?' x scalar @$reseller_id,1) . ')';
            push(@params,@$reseller_id);
        } else {
            $stmt .= ' AND contact.reseller_id = ?';
            push(@params,$reseller_id);
        }
    }
    $stmt .= ' ORDER BY r1.id ASC LIMIT 1';

    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    my $result = buildrecords_fromrows($rows,$load_recursive)->[0];
    die($stmt,join("-",@params)) unless $result;

    return $result;

}

sub countby_status_resellerid {

    my ($status,$reseller_id) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' AS subscriber' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contracts::gettablename()) . ' AS contract ON subscriber.contract_id = contract.id' .
    ' INNER JOIN ' . $db->tableidentifier(NGCP::BulkProcessor::Dao::Trunk::billing::contacts::gettablename()) . ' AS contact ON contract.contact_id = contact.id';
    my @params = ();
    my @terms = ();
    if ($status) {
        push(@terms,'subscriber.status = ?');
        push(@params,$status);
    }
    if ($reseller_id) {
        if ('ARRAY' eq ref $reseller_id) {
            push(@terms,'contact.reseller_id IN (' . substr(',?' x scalar @$reseller_id,1) . ')');
            push(@params,@$reseller_id);
        } else {
            push(@terms,'contact.reseller_id = ?');
            push(@params,$reseller_id);
        }
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
        my ($contract_id,
            $domain_id,
            $username,
            $uuid) = @params{qw/
                contract_id
                domain_id
                username
                uuid
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('contact_id') . ', ' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('domain_id') . ', ' .
                $db->columnidentifier('external_id') . ', ' .
                $db->columnidentifier('primary_number_id') . ', ' .
                $db->columnidentifier('status') . ', ' .
                $db->columnidentifier('username') . ', ' .
                $db->columnidentifier('uuid') . ') VALUES (' .
                'NULL, ' .
                '?, ' .
                '?, ' .
                'NULL, ' .
                'NULL, ' .
                '\'' . $ACTIVE_STATE . '\', ' .
                '?, ' .
                '?)',
                $contract_id,
                $domain_id,
                $username,
                $uuid,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

}

sub process_records {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $blocksize,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            blocksize
            numofthreads
            load_recursive
        /};

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return process_table(
        get_db                      => $get_db,
        class                       => __PACKAGE__,
        process_code                => sub {
                my ($context,$rowblock,$row_offset) = @_;
                return &$process_code($context,buildrecords_fromrows($rowblock,$load_recursive),$row_offset);
            },
        static_context              => $static_context,
        init_process_context_code   => $init_process_context_code,
        uninit_process_context_code => $uninit_process_context_code,
        destroy_reader_dbs_code     => \&destroy_dbs,
        multithreading              => $multithreading,
        blocksize                   => $blocksize,
        tableprocessing_threads     => $numofthreads,
        'select'                    => 'SELECT * FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"',
        'selectcount'               => 'SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $db->columnidentifier('status') . ' != "' . $TERMINATED_STATE . '"',
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
