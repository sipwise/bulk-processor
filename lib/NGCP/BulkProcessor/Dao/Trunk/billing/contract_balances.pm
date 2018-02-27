package NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances;
use strict;

## no critic

use DateTime qw();

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
    copy_row
);
use NGCP::BulkProcessor::SqlRecord qw();
use NGCP::BulkProcessor::Calendar qw(is_infinite_future infinite_future set_timezone);

use NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlRecord);
our @EXPORT_OK = qw(
    gettablename
    check_table
    insert_row
    update_row
    findby_contractid
    sort_by_end_desc
    sort_by_end_asc
    get_new_balance_values
    get_free_ratio
);

my $tablename = 'contract_balances';
my $get_db = \&get_billing_db;

my $expected_fieldnames = [
    'id',
    'contract_id',
    'cash_balance',
    'cash_balance_interval',
    'free_time_balance',
    'free_time_balance_interval',
    'topup_count',
    'timely_topup_count',
    'start',
    'end',
    'invoice_id',
    'underrun_profiles',
    'underrun_lock',
    'initial_cash_balance',
    'initial_free_time_balance',
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

sub findby_contractid {

    my ($xa_db,$contract_id,$load_recursive) = @_;

    check_table();
    my $db = &$get_db();
    $xa_db //= $db;
    my $table = $db->tableidentifier($tablename);

    my $stmt = 'SELECT * FROM ' . $table . ' WHERE ' .
            $db->columnidentifier('contract_id') . ' = ?';
    my @params = ($contract_id);

    my $rows = $xa_db->db_get_all_arrayref($stmt,@params);

    return buildrecords_fromrows($rows,$load_recursive);

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
        my ($contract_id) = @params{qw/
                contract_id
            /};

        if ($xa_db->db_do('INSERT INTO ' . $db->tableidentifier($tablename) . ' (' .
                $db->columnidentifier('cash_balance') . ', ' .
                $db->columnidentifier('cash_balance_interval') . ', ' .
                $db->columnidentifier('contract_id') . ', ' .
                $db->columnidentifier('end') . ', ' .
                $db->columnidentifier('free_time_balance') . ', ' .
                $db->columnidentifier('free_time_balance_interval') . ', ' .
                $db->columnidentifier('start') . ', ' .
                $db->columnidentifier('underrun_lock') . ', ' .
                $db->columnidentifier('underrun_profiles') . ') VALUES (' .
                '0.0, ' .
                '0.0, ' .
                '?, ' .
                'CONCAT(LAST_DAY(NOW()),\' 23:59:59\'), ' .
                '0, ' .
                '0, ' .
                'CONCAT(SUBDATE(CURDATE(),(DAY(CURDATE())-1)),\' 00:00:00\'), ' .
                'NULL, ' .
                'NULL)',
                $contract_id,
            )) {
            rowinserted($db,$tablename,getlogger(__PACKAGE__));
            return $xa_db->db_last_insert_id();
        }
    }
    return undef;

}

sub update_row {

    my ($xa_db,$data) = @_;

    check_table();
    return update_record($get_db,$xa_db,__PACKAGE__,$data);

}

sub buildrecords_fromrows {

    my ($rows,$load_recursive) = @_;

    my @records = ();
    my $record;

    if (defined $rows and ref $rows eq 'ARRAY') {
        my $db = &$get_db();
        foreach my $row (@$rows) {
            $record = __PACKAGE__->new($row);

            # transformations go here ...
            my $end = $db->datetime_from_string($record->{end},undef);
            if (is_infinite_future($end)) {
                $record->{_end} = infinite_future();
            } else {
                $record->{_end} = set_timezone($end);
            }

            $record->{_start} = $db->datetime_from_string($record->{start},'local');

            push @records,$record;
        }
    }

    return \@records;

}

sub sort_by_end_asc ($$) {
    return _sort_by_date('_end',0,@_);
}

sub sort_by_end_desc ($$) {
    return _sort_by_date('_end',1,@_);
}

sub _sort_by_date {
    my ($ts_field,$desc,$a,$b) = @_;
    if ($desc) {
        $desc = -1;
    } else {
        $desc = 1;
    }
    #use Data::Dumper;
    #print Dumper($a);
    #print Dumper($b);
    my $a_inf = is_infinite_future($a->{$ts_field});
    my $b_inf = is_infinite_future($b->{$ts_field});
    if ($a_inf and $b_inf) {
        return 0;
    } elsif ($a_inf) {
        return 1 * $desc;
    } elsif ($b_inf) {
        return -1 * $desc;
    } else {
        return DateTime->compare($a->{$ts_field}, $b->{$ts_field}) * $desc;
    }

}

sub get_new_balance_values {
    my %params = @_;
    my ($contract_create,
        $last_balance,
        $balance,
        $initial_balance,
        $carry_over_mode,
        $notopup_expiration,
        $last_cash_balance,
        $last_cash_balance_interval) = @params{qw/
        contract_create
        last_balance
        balance
        initial_balance
        carry_over_mode
        notopup_expiration
        last_cash_balance
        last_cash_balance_interval
    /};
    my ($cash_balance, $cash_balance_interval, $free_time_balance, $free_time_balance_interval) = (0.0,0.0,0,0);

    $carry_over_mode //= $NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages::DEFAULT_CARRY_OVER_MODE;
    my $ratio;
    if ($last_balance) {
        $last_cash_balance //= $last_balance->{cash_balance};
        $last_cash_balance_interval //= $last_balance->{cash_balance_interval};
        if (($NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages::CARRY_OVER_MODE eq $carry_over_mode
             || ($NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages::CARRY_OVER_TIMELY_MODE eq $carry_over_mode && $last_balance->{timely_topup_count} > 0)
            ) && (!defined $notopup_expiration || $balance->{_start} < $notopup_expiration)) {
            #if (!defined $last_profile) {
            #    my $bm_last = get_actual_billing_mapping(schema => $schema, contract => $contract, now => $last_balance->start); #end); !?
            #    $last_profile = $bm_last->billing_mappings->first->billing_profile;
            #}
            #my $contract_create = NGCP::Panel::Utils::DateTime::set_local_tz($contract->create_timestamp // $contract->modify_timestamp);
            $ratio = 1.0;
            if ($last_balance->{_start} <= $contract_create && $last_balance->{_end} >= $contract_create) { #$last_balance->end is never +inf here
                $ratio = get_free_ratio($contract_create,$last_balance->{_start},$last_balance->{_end});
            }
            my $old_free_cash = $ratio * ($last_balance->{_profile}->{interval_free_cash} // $NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::DEFAULT_PROFILE_FREE_CASH);
            $cash_balance = $last_cash_balance;
            if ($last_cash_balance_interval < $old_free_cash) {
                $cash_balance += $last_cash_balance_interval - $old_free_cash;
            }
            #$ratio * $last_profile->interval_free_time // _DEFAULT_PROFILE_FREE_TIME
        #} else {
        #    $c->log->debug('discarding contract ' . $contract->id . " cash balance (mode '$carry_over_mode'" . (defined $notopup_expiration ? ', notopup expiration ' . NGCP::Panel::Utils::DateTime::to_string($notopup_expiration) : '') . ')') if $c;
        }
        $ratio = 1.0;
    } else {
        $cash_balance = (defined $initial_balance ? $initial_balance : $NGCP::BulkProcessor::Dao::Trunk::billing::profile_packages::DEFAULT_INITIAL_BALANCE);
        $ratio = get_free_ratio($contract_create,$balance->{_start},$balance->{_end});
    }

    my $free_cash = $ratio * ($balance->{_profile}->{interval_free_cash} // $NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::DEFAULT_PROFILE_FREE_CASH);
    $cash_balance += $free_cash;
    $cash_balance_interval = 0.0;

    my $free_time = $ratio * ($balance->{_profile}->{interval_free_time} // $NGCP::BulkProcessor::Dao::Trunk::billing::billing_profiles::DEFAULT_PROFILE_FREE_TIME);
    $free_time_balance = $free_time;
    $free_time_balance_interval = 0;

    #$c->log->debug("ratio: $ratio, free cash: $free_cash, cash balance: $cash_balance, free time: $free_time, free time balance: $free_time_balance");

    return {cash_balance => sprintf("%.4f",$cash_balance),
            initial_cash_balance => sprintf("%.4f",$cash_balance),
            cash_balance_interval => sprintf("%.4f",$cash_balance_interval),
            free_time_balance => sprintf("%.0f",$free_time_balance),
            initial_free_time_balance => sprintf("%.0f",$free_time_balance),
            free_time_balance_interval => sprintf("%.0f",$free_time_balance_interval)};

}

sub get_free_ratio {
    my ($contract_create,$stime,$etime) = @_;
    if (!is_infinite_future($etime)) {
        my $ctime = ($contract_create->clone->truncate(to => 'day') > $stime ? $contract_create->clone->truncate(to => 'day') : $contract_create);
        my $start_of_next_interval = _add_second($etime->clone,1);
        #$c->log->debug("ratio = " . ($start_of_next_interval->epoch - $ctime->epoch) . ' / ' . ($start_of_next_interval->epoch - $stime->epoch)) if $c;
        return ($start_of_next_interval->epoch - $ctime->epoch) / ($start_of_next_interval->epoch - $stime->epoch);
    }
    return 1.0;
}

sub _add_second {

    my ($dt,$skip_leap_seconds) = @_;
    $dt->add(seconds => 1);
    while ($skip_leap_seconds and $dt->second() >= 60) {
        $dt->add(seconds => 1);
    }
    return $dt;

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
