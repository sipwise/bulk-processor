package NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    get_import_db
    destroy_all_dbs
);
#import_db_tableidentifier

use NGCP::BulkProcessor::SqlProcessor qw(
    registertableinfo
    create_targettable
    checktableinfo
    copy_row

    insert_stmt

    process_table
);
use NGCP::BulkProcessor::SqlRecord qw();

#use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    create_table
    gettablename
    check_table
    getinsertstatement
    getupsertstatement

    findby_number
    countby_number

    update_delta
    findby_delta
    countby_delta

    $deleted_delta
    $updated_delta
    $added_delta

    process_records

    @fieldnames
    @contact_fieldnames
);

my $tablename = 'subscriber';
my $get_db = \&get_import_db;
#my $get_tablename = \&import_db_tableidentifier;

our @contact_fieldnames = (
    "first_name", #"First name",
    "last_name", #Last name",
    "company", #"Company",
    "company_registration_number", #"Company registration number",
    "street", #"Street",
    "postal_code", #"Postal code",
    "city_name", #"City name",
    "phone_number", #"Phone number",
    "email", #"Email",
    "vat_number", #"VAT number"
);
our @fieldnames = (
    "reseller_name", #"Reseller name",
    "domain", #"Sip domain name",
    "customer_id", #"Customer ID",
    "billing_profile_name", #"Billing profile name",
    "sip_username", #"Subscriber sip username",
    "sip_password", #"Subscriber sip password",
    "cc", #"Subscriber primary number - country code (cc)",
    "ac", #"Subscriber primary number - country code (ac)",
    "sn", #"Subscriber primary number - country code (sn)",
    'range',
    "web_username", #"Subscriber web username",
    "web_password", #"Subscriber web password",
    @contact_fieldnames,
    'contact_hash',
    "barrings",
    "allowed_ips",
    "channels",
    "voicemail",
);
my $expected_fieldnames = [
    @fieldnames,
    'delta',
];

# table creation:
my $primarykey_fieldnames = []; #[ 'cc','ac','sn' ];
my $indexes = {
    $tablename . '_reseller_name' => [ 'reseller_name(32)' ],
    $tablename . '_customer_id' => [ 'customer_id(32)' ],
    $tablename . '_billing_profile_name' => [ 'billing_profile_name(64)' ],
    $tablename . '_sip_username_password' => [ 'sip_username(32),sip_password(32)' ],
    $tablename . '_web_username_password' => [ 'web_username(32),web_password(32)' ],
    $tablename . '_delta' => [ 'delta(7)' ],
};
#my $fixtable_statements = [];

our $deleted_delta = 'DELETED';
our $updated_delta = 'UPDATED';
our $added_delta = 'ADDED';

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
    return create_targettable($db,__PACKAGE__,$db,__PACKAGE__,$tablename,$truncate,0,undef);

}

sub findby_delta {

    my ($delta,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless defined $delta;

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('delta') . ' = ?'
    ,$delta);

    return buildrecords_fromrows($rows,$load_recursive);

}

sub findby_ccacsn {

    my ($cc,$ac,$sn,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    return [] unless (defined $cc or defined $ac or defined $sn);

    my $rows = $db->db_get_all_arrayref(
        'SELECT * FROM ' .
            $table .
        ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?'
    ,$cc,$ac,$sn);

    return buildrecords_fromrows($rows,$load_recursive)->[0];

}

sub update_delta {

    my ($cc,$ac,$sn,$delta) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'UPDATE ' . $table . ' SET delta = ?';
    my @params = ();
    push(@params,$delta);
    if (defined $cc or defined $ac or defined $sn) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?';
        push(@params,$cc,$ac,$sn);
    }

    return $db->db_do($stmt,@params);

}

sub countby_ccacsn {

    my ($cc,$ac,$sn) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table;
    my @params = ();
    if (defined $cc or defined $ac or defined $sn) {
        $stmt .= ' WHERE ' .
            $db->columnidentifier('cc') . ' = ?' .
            ' AND ' . $db->columnidentifier('ac') . ' = ?' .
            ' AND ' . $db->columnidentifier('sn') . ' = ?';
        push(@params,$cc,$ac,$sn);
    }

    return $db->db_get_value($stmt,@params);

}

sub countby_delta {

    my ($deltas) = @_;

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT COUNT(*) FROM ' . $table . ' WHERE 1=1';
    my @params = ();
    if (defined $deltas and 'HASH' eq ref $deltas) {
        foreach my $in (keys %$deltas) {
            my @values = (defined $deltas->{$in} and 'ARRAY' eq ref $deltas->{$in} ? @{$deltas->{$in}} : ($deltas->{$in}));
            $stmt .= ' AND ' . $db->columnidentifier('delta') . ' ' . $in . ' (' . substr(',?' x scalar @values,1) . ')';
            push(@params,@values);
        }
    } elsif (defined $deltas and length($deltas) > 0) {
        $stmt .= ' AND ' . $db->columnidentifier('delta') . ' = ?';
        push(@params,$deltas);
    }

    return $db->db_get_value($stmt,@params);

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

sub process_records {

    my %params = @_;
    my ($process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            load_recursive
        /};

    check_table();

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
        destroy_reader_dbs_code     => \&destroy_all_dbs,
        multithreading              => $multithreading,
        tableprocessing_threads     => $numofthreads,
    );
}

sub getinsertstatement {

    my ($insert_ignore) = @_;
    check_table();
    return insert_stmt($get_db,__PACKAGE__,$insert_ignore);

}

sub getupsertstatement {

    check_table();
    my $db = &$get_db();
    my $table = $db->tableidentifier($tablename);
    my $upsert_stmt = 'INSERT OR REPLACE INTO ' . $table . ' (' .
      join(', ',map { local $_ = $_; $_ = $db->columnidentifier($_); $_; } @$expected_fieldnames) . ')';
    my @values = ();
    foreach my $fieldname (@$expected_fieldnames) {
        if ('delta' eq $fieldname) {
            my $stmt = 'SELECT \'' . $updated_delta . '\' FROM ' . $table . ' WHERE ' .
                $db->columnidentifier('cc') . ' = ?' .
                ' AND ' . $db->columnidentifier('ac') . ' = ?' .
                ' AND ' . $db->columnidentifier('sn') . ' = ?';
            push(@values,'COALESCE((' . $stmt . '), \'' . $added_delta . '\')');
        } else {
            push(@values,'?');
        }
    }
    $upsert_stmt .= ' VALUES (' . join(',',@values) . ')';
    return $upsert_stmt;

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
