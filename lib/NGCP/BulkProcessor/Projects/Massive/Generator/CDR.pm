package NGCP::BulkProcessor::Projects::Massive::Generator::CDR;
use strict;

## no critic

use threads::shared qw();
use Time::HiRes qw(sleep);
use String::MkPasswd qw();
#use List::Util qw();
use Data::Rmap qw();

use Tie::IxHash;

use NGCP::BulkProcessor::Globals qw(
    $enablemultithreading
);

use NGCP::BulkProcessor::Projects::Massive::Generator::Settings qw(
    $dry
    $skip_errors
    $deadlock_retries

    $generate_cdr_multithreading
    $generate_cdr_numofthreads
    $generate_cdr_count

    @providers
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
    fileerror
);

use NGCP::BulkProcessor::Dao::Trunk::accounting::cdr qw();

use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contacts qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
    ping_dbs
    destroy_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid timestamp); # stringtobool check_ipnet trim);
#use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
#use NGCP::BulkProcessor::RandomString qw(createtmpstring);
use NGCP::BulkProcessor::Array qw(array_to_map);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    generate_cdrs

);

my $thread_sleep_secs = 0.1;

my $RUNNING = 1;
my $COMPLETED = 2;
my $ERROR = 4;
my $STOP = 8;

my $total_count :shared = 0;

my $t = time;
my %offnet_domain_subscriber_map : shared = ();

sub generate_cdrs {

    my $context = {};
    my $result = _generate_cdrs_create_context($context);

    destroy_dbs();
    if ($result) {
        if ($enablemultithreading and $generate_cdr_multithreading and $generate_cdr_count > 1) {
            $context->{cdr_count} = int($generate_cdr_count / $generate_cdr_numofthreads);
            #$context->{sn_increment} = $generate_cdr_numofthreads;
            my %processors = ();
            for (my $i = 0; $i < $generate_cdr_numofthreads; $i++) {
                $context->{cdr_count} += ($generate_cdr_count - $context->{cdr_count} * $generate_cdr_numofthreads) if $i == 0;
                _info($context,'starting generator thread ' . ($i + 1) . ' of ' . $generate_cdr_numofthreads);
                $context->{sn_offset} = $i;
                my $processor = threads->create(\&_generate_cdr,$context);
                if (!defined $processor) {
                    _info($context,'generator thread ' . ($i + 1) . ' of ' . $generate_cdr_numofthreads . ' NOT started');
                }
                $processors{$processor->tid()} = $processor;
            }
            local $SIG{'INT'} = sub {
                _info($context,"interrupt signal received");
                $result = 0;
                lock $context->{errorstates};
                $context->{errorstates}->{$context->{tid}} = $STOP;
            };
            while ((scalar keys %processors) > 0) {
                foreach my $processor (values %processors) {
                    if (defined $processor and $processor->is_joinable()) {
                        $processor->join();
                        delete $processors{$processor->tid()};
                        _info($context,'generator thread tid ' . $processor->tid() . ' joined');
                    }
                }
                sleep($thread_sleep_secs);
            }

            $result &= (_get_threads_state($context->{errorstates},$context->{tid}) & $COMPLETED) == $COMPLETED;

        } else {

            $context->{cdr_count} = $generate_cdr_count;
            #$context->{sn_increment} = 1;
            #$context->{sn_offset} = 0;
            local $SIG{'INT'} = sub {
                _info($context,"interrupt signal received");
                $context->{errorstates}->{$context->{tid}} = $STOP;
            };
            $result = _generate_cdr($context);

        }
    }

    return $result;
}

sub _generate_cdr {

    my $context = shift;
    my $tid = threadid();
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }
    $context->{tid} = $tid;
    $context->{db} = &get_xa_db();

    my $cdr_count = 0;
    my $broadcast_state;
    while (($broadcast_state = _get_threads_state($context->{errorstates})) == 0
           or
           (($broadcast_state & $ERROR) == 0
           and ($broadcast_state & $STOP) == 0)) {

        last if $cdr_count >= $context->{cdr_count};
        $cdr_count += 1;

        eval {
            next unless _generate_cdr_init_context($context);
        };
        if ($@ and not $skip_errors) {
            undef $context->{db};
            destroy_dbs();
            lock $context->{errorstates};
            $context->{errorstates}->{$tid} = $ERROR;
            return 0;
        }

        my $retry = 1;
        while ($retry > 0) {
        eval {
            $context->{db}->db_begin();

            _create_cdr($context);

            {
                #lock $db_lock; #concurrent writes to voip_numbers causes deadlocks
                lock $total_count;
                $total_count += 1;
                _info($context,"$total_count CDRs created",($total_count % 10) > 0);
            }

            if ($dry) {
                $context->{db}->db_rollback(0);
            } else {
                $context->{db}->db_commit();
            }
        };
        my $err = $@;
        if ($err) {
            eval {
                $context->{db}->db_rollback(1);
            };
            if ($err =~ /deadlock/gi and $retry < $deadlock_retries) {
                my $sleep = 0.01 * 2**$retry;
                _info($context,"retrying in $sleep secs");
                sleep($sleep);
                $retry += 1;
            } elsif (not $skip_errors) {
                undef $context->{db};
                destroy_dbs();
                lock $context->{errorstates};
                $context->{errorstates}->{$tid} = $ERROR;
                return 0;
            }
        } else {
            $retry = 0;
        }
        }
    }
    undef $context->{db};
    destroy_dbs();
    if (($broadcast_state & $ERROR) == $ERROR) {
        _info($context,"shutting down (error broadcast)");
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $STOP;
        return 0;
    } elsif (($broadcast_state & $STOP) == $STOP) {
        _info($context,"shutting down (stop broadcast)");
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $STOP;
        return 0;
    } else {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $COMPLETED;
        return 1;
    }

}


sub _generate_cdrs_create_context {
    my ($context) = @_;

    my $result = 1;

    my %errorstates :shared = ();
    my $tid = threadid();
    $context->{tid} = $tid;
    $context->{now} = timestamp();
    $context->{errorstates} = \%errorstates;
    {
        lock $context->{errorstates};
        $context->{errorstates}->{$tid} = $RUNNING;
    }
    $context->{error_count} = 0;
    $context->{warning_count} = 0;

    my $result = 1;

    my @reseller_ids = map { $_->{reseller}->{id}; } @providers;
    $context->{reseller_ids} = \@reseller_ids;

    my $domain_count = 0;
    eval {
        ($context->{domain_map},my $ids,my $domains) = array_to_map(NGCP::BulkProcessor::Dao::Trunk::billing::domains::findall(),
            sub { return shift->{id}; }, sub { return shift; }, 'first' );
        $domain_count = (scalar keys %{$context->{domain_map}});
    };
    if ($@ or $domain_count == 0) {
        _error($context,"cannot find any domains");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$domain_count domains cached");
    }

    my $reseller_count = 0;
    eval {
        ($context->{reseller_map},my $ids,my $resellers) = array_to_map(NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findall(),
            sub { return shift->{id}; }, sub { return shift; }, 'first' );
        $reseller_count = (scalar keys %{$context->{reseller_map}});
    };
    if ($@ or $reseller_count == 0) {
        _error($context,"cannot find any resellers");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$reseller_count resellers cached");
    }

    my $active_count = 0;
    eval {
        $active_count = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::countby_status_resellerid(
            $NGCP::BulkProcessor::Dao::Trunk::billing::contracts::ACTIVE_STATE,
            ((scalar @{$context->{reseller_ids}}) > 0 ? $context->{reseller_ids} : undef)
        );
        ($context->{min_id},$context->{max_id}) = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::find_minmaxid(undef,
            { 'IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::ACTIVE_STATE },
            ((scalar @{$context->{reseller_ids}}) > 0 ? $context->{reseller_ids} : undef)
        );
    };
    if ($@ or $active_count == 0) {
        _error($context,"cannot find active subscribers");
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,"$active_count active subscribers found");
    }

    return $result;

}

sub _create_cdr {
    my ($context) = @_;

    NGCP::BulkProcessor::Dao::Trunk::accounting::cdr::insert_row($context->{db},$context->{cdr});

    return 1;

}

sub _generate_cdr_init_context {

    my ($context) = @_;

    my $result = 1;

    #my $provider = $providers[rand @providers];

    my $offnet_in;
    my $offnet_out;

	my $source_subscriber;
    $source_subscriber = _get_random_subscriber($context) unless $offnet_in;
    my $dest_subscriber;
    $dest_subscriber = _get_random_subscriber($context,(defined $source_subscriber ? $source_subscriber->{id} : undef)) unless $offnet_out;

    my $source_peering_subscriber_info;
    my $source_reseller;
    if ($source_subscriber) {
        $source_subscriber->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id($source_subscriber->{contract_id});
        $source_subscriber->{contract}->{contact} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_id($source_subscriber->{contract}->{contact_id});
        $source_subscriber->{contract}->{prov_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$source_subscriber->{uuid});
        $source_subscriber->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($source_subscriber->{contract}->{prov_subscriber}->{id},1);
        $source_subscriber->{domain} = $context->{domain_map}->{$source_subscriber->{domain_id}}->{domain};
        $source_reseller = $context->{reseller_map}->{$source_subscriber->{contract}->{contact}->{reseller_id}};
    } else {
        $source_peering_subscriber_info = _prepare_offnet_subscriber_info();
    }

    my $dest_peering_subscriber_info;
    my $dest_reseller;
    if ($dest_subscriber) {
        $dest_subscriber->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts::findby_id($dest_subscriber->{contract_id});
        $dest_subscriber->{contract}->{contact} = NGCP::BulkProcessor::Dao::Trunk::billing::contacts::findby_id($dest_subscriber->{contract}->{contact_id});
        $dest_subscriber->{contract}->{prov_subscriber} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::findby_uuid(undef,$dest_subscriber->{uuid});
        $dest_subscriber->{primary_alias} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::findby_subscriberidisprimary($dest_subscriber->{contract}->{prov_subscriber}->{id},1);
        $dest_subscriber->{domain} = $context->{domain_map}->{$dest_subscriber->{domain_id}}->{domain};
        $dest_reseller = $context->{reseller_map}->{$dest_subscriber->{contract}->{contact}->{reseller_id}};
    } else {
        $dest_peering_subscriber_info = _prepare_offnet_subscriber_info();
    }

    my $source_ip = '192.168.0.1';
    my $time = time();
    my $duration = 120;

	$context->{cdr} = {
		#id                                     => ,
		#update_time                            => ,
		source_user_id                          => ($source_subscriber ? $source_subscriber->{uuid} : '0'),
		source_provider_id                      => ($source_reseller ? $source_reseller->{contract_id} : '0'),
		#source_external_subscriber_id          => ,
		#source_external_contract_id            => ,
		source_account_id                       => ($source_subscriber ? $source_subscriber->{contract_id} : '0'),
		source_user                             => ($source_subscriber ? $source_subscriber->{username} : $source_peering_subscriber_info->{username}),
		source_domain                           => ($source_subscriber ? $source_subscriber->{domain} : $source_peering_subscriber_info->{domain}),
		source_cli                              => ($source_subscriber ? ($source_subscriber->{primary_alias}->{username} // $source_subscriber->{username}) : $source_peering_subscriber_info->{username}),
		#source_clir                            => '0',
		source_ip                               => $source_ip,
		#source_gpp0                            => ,
		#source_gpp1                            => ,
		#source_gpp2                            => ,
		#source_gpp3                            => ,
		#source_gpp4                            => ,
		#source_gpp5                            => ,
		#source_gpp6                            => ,
		#source_gpp7                            => ,
		#source_gpp8                            => ,
		#source_gpp9                            => ,
		destination_user_id                     => ($dest_subscriber ? $dest_subscriber->{uuid} : '0'),
		destination_provider_id                 => ($dest_reseller ? $dest_reseller->{contract_id} : '0'),
		#destination_external_subscriber_id     => ,
		#destination_external_contract_id       => ,
		destination_account_id                  => ($dest_subscriber ? $dest_subscriber->{contract_id} : '0'),
		destination_user                        => ($dest_subscriber ? $dest_subscriber->{username} : $dest_peering_subscriber_info->{username}),
		destination_domain                      => ($dest_subscriber ? $dest_subscriber->{domain} : $dest_peering_subscriber_info->{domain}),
		destination_user_dialed                 => ($dest_subscriber ? ($dest_subscriber->{primary_alias}->{username} // $dest_subscriber->{username}) : $dest_peering_subscriber_info->{username}),
		destination_user_in                     => ($dest_subscriber ? ($dest_subscriber->{primary_alias}->{username} // $dest_subscriber->{username}) : $dest_peering_subscriber_info->{username}),
		destination_domain_in                   => ($dest_subscriber ? $dest_subscriber->{domain} : $dest_peering_subscriber_info->{domain}),
		#destination_gpp0                       => ,
		#destination_gpp1                       => ,
		#destination_gpp2                       => ,
		#destination_gpp3                       => ,
		#destination_gpp4                       => ,
		#destination_gpp5                       => ,
		#destination_gpp6                       => ,
		#destination_gpp7                       => ,
		#destination_gpp8                       => ,
		#destination_gpp9                       => ,
		#peer_auth_user                         => ,
		#peer_auth_realm                        => ,
		call_type                               => 'call',
		call_status                             => 'ok',
		call_code                               => '200',
		init_time                               => $time,
		start_time                              => $time,
		duration                                => $duration,
		call_id                                 => _generate_call_id(),
		#source_carrier_cost                    => ,
		#source_reseller_cost                   => ,
		#source_customer_cost                   => ,
		#source_carrier_free_time               => ,
		#source_reseller_free_time              => ,
		#source_customer_free_time              => ,
		#source_carrier_billing_fee_id          => ,
		#source_reseller_billing_fee_id         => ,
		#source_customer_billing_fee_id         => ,
		#source_carrier_billing_zone_id         => ,
		#source_reseller_billing_zone_id        => ,
		#source_customer_billing_zone_id        => ,
		#destination_carrier_cost               => ,
		#destination_reseller_cost              => ,
		#destination_customer_cost              => ,
		#destination_carrier_free_time          => ,
		#destination_reseller_free_time         => ,
		#destination_customer_free_time         => ,
		#destination_carrier_billing_fee_id     => ,
		#destination_reseller_billing_fee_id    => ,
		#destination_customer_billing_fee_id    => ,
		#destination_carrier_billing_zone_id    => ,
		#destination_reseller_billing_zone_id   => ,
		#destination_customer_billing_zone_id   => ,
		#frag_carrier_onpeak                    => ,
		#frag_reseller_onpeak                   => ,
		#frag_customer_onpeak                   => ,
		#is_fragmented                          => ,
		#split                                  => ,
		#rated_at                               => ,
		#rating_status                          => 'unrated',
		#exported_at                            => ,
		#export_status                          => ,
	};

    return $result;

}

sub _prepare_offnet_subscriber_info {
	my ($username_primary_number,$domain) = @_;
    lock %offnet_domain_subscriber_map;
	my $n = 1 + scalar keys %offnet_domain_subscriber_map;
	Data::Rmap::rmap { $_ =~ s/<n>/$n/; $_ =~ s/<i>/$n/; $_ =~ s/<t>/$t/; } ($domain);
	$n = 1 + (exists $offnet_domain_subscriber_map{$domain} ? scalar keys %{$offnet_domain_subscriber_map{$domain}} : 0);
	Data::Rmap::rmap { $_ =~ s/<n>/$n/; $_ =~ s/<i>/$n/; $_ =~ s/<t>/$t/; } ($username_primary_number);
	my $username;
	if ('HASH' eq ref $username_primary_number) {
        $username = ($username_primary_number->{cc} // '') . ($username_primary_number->{ac} // '') . ($username_primary_number->{sn} // '');
    } else {
		$username = $username_primary_number;
	}
	$offnet_domain_subscriber_map{$domain} = {} if not exists $offnet_domain_subscriber_map{$domain};
	$offnet_domain_subscriber_map{$domain}->{$username} = 1;
	return { username => $username, domain => $domain };
}

sub _get_random_subscriber {
    my ($context,$excluding_id) = @_;

    return NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::find_random(
        $context->{db},
        $excluding_id,
        { 'IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::ACTIVE_STATE },
        ((scalar @{$context->{reseller_ids}}) > 0 ? $context->{reseller_ids} : undef),
        $context->{min_id},$context->{max_id},
    );
}

sub _generate_call_id {
	return '*TEST*'._random_string(26,'a'..'z','A'..'Z',0..9,'-','.');
}

sub _random_string {
	my ($length,@chars) = @_;
	return join('',@chars[ map{ rand @chars } 1 .. $length ]);
}

sub _get_threads_state {
    my ($errorstates,$tid) = @_;
    my $result = 0;
    if (defined $errorstates and ref $errorstates eq 'HASH') {
        lock $errorstates;
        foreach my $threadid (keys %$errorstates) {
            if (not defined $tid or $threadid != $tid) {
                $result |= $errorstates->{$threadid};
            }
        }
    }
    return $result;
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;