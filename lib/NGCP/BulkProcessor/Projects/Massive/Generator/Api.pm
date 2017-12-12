package NGCP::BulkProcessor::Projects::Massive::Generator::Api;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
use Data::Rmap qw();

use NGCP::BulkProcessor::Projects::Massive::Generator::Settings qw(
    $dry
    $skip_errors
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::RestRequests::Trunk::SystemContacts qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Contracts qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Resellers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::Domains qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingZones qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingFees qw();

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    setup_provider
);

my $t = time;
my %entity_maps = ();

sub setup_provider {
    my %params = @_;
	my (
        $domain_name,
        $reseller_name,
        $subscriber_rates,
        $provider_rate,
        $type
    ) = @params{qw/
            domain
            reseller
            subscriber_rates
            provider_rate
            type
        /};
	my $provider = {};

    $provider->{reseller} = _find_entity('NGCP::BulkProcessor::RestRequests::Trunk::Resellers',
        name => $reseller_name,
    );
    my $new_reseller = 0;
    if (defined $provider->{reseller}) {
        _info("reseller '$reseller_name' found");
        $provider->{contract} = NGCP::BulkProcessor::RestRequests::Trunk::Contracts::get_item($provider->{reseller}->{contract_id});
        if (defined $provider->{contract}) {
            _info("contract ID $provider->{reseller}->{contract_id} found");
        } else {
            _info("contract ID $provider->{reseller}->{contract_id} not found");
            return undef;
        }

        $provider->{contact} = NGCP::BulkProcessor::RestRequests::Trunk::SystemContacts::get_item($provider->{contract}->{contact_id});
        if (defined $provider->{contact}) {
            _info("contact ID $provider->{contract}->{contact_id} found");
        } else {
            _info("contact ID $provider->{contract}->{contact_id} not found");
            return undef;
        }
    } elsif (not $dry) {
        $provider->{contact} = _create_systemcontact();
        _info("contact ID $provider->{contact}->{id} created");
        $provider->{contract} = _create_contract(
            contact_id => $provider->{contact}->{id},
            billing_profile_id => 1, #default profile id
            type => $type // 'reseller',
        );
        _info("contract ID $provider->{contract}->{id} created");
        $provider->{reseller} = _create_reseller(
            contract_id => $provider->{contract}->{id},
            name => $reseller_name, #"test <t> <n>",
        );
        _info("reseller '$reseller_name' created");
        $new_reseller = 1;
    } else {
        _info("reseller '$reseller_name' not found");
        return undef;
    }

    $provider->{domain} = _find_entity('NGCP::BulkProcessor::RestRequests::Trunk::Domains',
        domain => $domain_name,
    );
    if (defined $provider->{domain}) {
        _info("domain '$domain_name' found");
    } elsif (not $dry) {
        $provider->{domain} = _create_domain(
            reseller_id => $provider->{reseller}->{id},
            #domain => $domain_name.'.<t>',
            domain => $domain_name,
        );
        _info("domain '$domain_name' created");
    } else {
        _info("domain '$domain_name' not found");
        return undef;
    }

    my $provider_profile = NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles::get_item($provider->{contract}->{billing_profile_id});
    if (not $new_reseller and defined $provider_profile) {
        _info("provider billing profile ID $provider_profile->{id} found");
        my $profile_fee = {};
            ($profile_fee->{profile},
             $profile_fee->{zone},
             $profile_fee->{fee},
             $profile_fee->{fees}) = _load_fees($provider_profile);
            $provider->{profile} = $profile_fee->{profile};
            $provider->{provider_fee} = $profile_fee;
    } elsif (not $dry) {
        if (defined $provider_rate) {
            my $profile_fee = {};
            ($profile_fee->{profile},
             $profile_fee->{zone},
             $profile_fee->{fee},
             $profile_fee->{fees}) = _setup_fees($provider->{reseller},
                %$provider_rate
            );
            $provider->{profile} = $profile_fee->{profile};
            $provider->{provider_fee} = $profile_fee;
            $provider->{contract} = _update_contract(
                id => $provider->{contract}->{id},
                billing_profile_id => $provider->{profile}->{id},
            );
            _info("contract ID $provider->{contract}->{id} updated");
        }
    } else {
        _info("provider billing profile ID $provider->{contract}->{billing_profile_id} not found");
        return undef;
    }

    $provider->{subscriber_fees} = [];
    foreach my $subscriber_profile (@{NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles::findby_resellerid($provider->{reseller}->{id})}) {
        next if (defined $provider_profile and $provider_profile->{id} == $subscriber_profile->{id});
        _info("subscriber billing profile ID $subscriber_profile->{id} found");
        my $profile_fee = {};
           ($profile_fee->{profile},
            $profile_fee->{zone},
            $profile_fee->{fee},
            $profile_fee->{fees}) = _load_fees($subscriber_profile);
        push(@{$provider->{subscriber_fees}},$profile_fee);
    }
    if ((scalar @{$provider->{subscriber_fees}}) == 0 and defined $subscriber_rates and (scalar @$subscriber_rates) > 0) {
        if (not $dry) {
            foreach my $rate (@$subscriber_rates) {
                my $profile_fee = {};
                ($profile_fee->{profile},
                 $profile_fee->{zone},
                 $profile_fee->{fee},
                 $profile_fee->{fees}) = _setup_fees($provider->{reseller},
                    %$rate
                );
                push(@{$provider->{subscriber_fees}},$profile_fee);
            }
        } else {
            _info("no subscriber billing profile(s) found");
            return undef;
        }
    }

	return $provider;
}

sub _load_fees {

    my ($profile) = @_;
    my $result = 1;
    my $zone = NGCP::BulkProcessor::RestRequests::Trunk::BillingZones::findby_billingprofileid($profile->{id})->[0];
    $result &= defined $profile;
    _info("billing zone ID $zone->{id} found") if $result;
    my $fees = [];
    $fees = NGCP::BulkProcessor::RestRequests::Trunk::BillingFees::findby_billingprofileid($profile->{id}) if $result;
    foreach my $fee (@$fees) {
        _info("billing fee ID $fee->{id} found");
    }
    return ($profile,$zone,$fees->[0],$fees);

}

sub _setup_fees {
	my ($reseller,%params) = @_;
	my $prepaid = delete $params{prepaid};
	my $peaktime_weekdays = delete $params{peaktime_weekdays};
	my $peaktime_specials = delete $params{peaktime_special};
	my $interval_free_time = delete $params{interval_free_time};
	#my $interval_free_cash = delete $params{interval_free_cash};
	my $profile = _create_billing_profile(
		reseller_id => $reseller->{id},
		(defined $prepaid ? (prepaid => $prepaid) : ()),
		(defined $peaktime_weekdays ? (peaktime_weekdays => $peaktime_weekdays) : ()),
		(defined $peaktime_specials ? (peaktime_special => $peaktime_specials) : ()),
		(defined $interval_free_time ? (interval_free_time => $interval_free_time) : ()),
		#(defined $interval_free_cash ? (interval_free_cash => $interval_free_cash) : ()),
	);
    _info("billing profile ID $profile->{id} created");
	my $zone = _create_billing_zone(
		billing_profile_id => $profile->{id},
	);
    _info("billing zone ID $profile->{id} created");
	my @fees = ();
	if (exists $params{fees}) {
		foreach my $fee (@{ $params{fees} }) {
			push(@fees,_create_billing_fee(
				billing_profile_id => $profile->{id},
				billing_zone_id => $zone->{id},
				%$fee,
			));
		}
	} else {
		push(@fees,_create_billing_fee(
			billing_profile_id => $profile->{id},
			billing_zone_id => $zone->{id},
			direction               => "out",
			destination             => ".",
			%params,
		));
	}
	return ($profile,$zone,$fees[0],\@fees);
}

sub _create_systemcontact {

    return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::SystemContacts',
        firstname => "syst_contact_<n>_first",
		lastname  => "syst_contact_<n>_last",
		email     => "syst_contact<n>\@custcontact.invalid",
		@_,
    );

}

sub _create_contract {

    return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::Contracts',
		status => "active",
		type => "reseller",
		@_,
    );

}

sub _update_contract {

    return _update_entity('NGCP::BulkProcessor::RestRequests::Trunk::Contracts',
        id => undef,
		@_,
    );

}

sub _create_reseller {

    return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::Resellers',
		status => "active",
        name => "test <t> <n>",
		@_,
    );

}

sub _create_domain {

	return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::Domains',
		domain => 'test_<t>_<n>.example.org',
		#reseller_id => $default_reseller_id,
		@_,
	);

}

sub _create_billing_profile {

	return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles',
		name => "test <t> <n>",
		handle  => "test_<t>_<n>",
		#reseller_id => $default_reseller_id,
		@_,
	);

}

sub _create_billing_zone {

	return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::BillingZones',
		zone => 'test<n>',
		detail => 'test <n>',
		@_,
	);

}

sub _create_billing_fee {

    my $fee = _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::BillingFees',
		@_,
	);
    _info("billing fee ID $fee->{id} created");
    return $fee;

}

sub _create_entity {

    my $class = shift;
    my (@params) = @_;
    my $map = _get_entity_map($class);
	my $n = 1 + scalar keys %$map;
	Data::Rmap::rmap { $_ =~ s/<n>/$n/ if defined $_; $_ =~ s/<i>/$n/ if defined $_; $_ =~ s/<t>/$t/ if defined $_; } @params;
    no strict 'refs';
    my $entity = &{$class . '::create_item'}({@params},1);
    $map->{$entity->{id}} = $entity;
    return $entity;

}

sub _update_entity {

    my $class = shift;
    my (@params) = @_;
    my $map = _get_entity_map($class);
	#my $n = 1 + scalar keys %$map;
	Data::Rmap::rmap { $_ =~ s/<t>/$t/ if defined $_; } @params;
    my $data = {@params};
    my $id = delete $data->{id};
    no strict 'refs';
    my $entity = &{$class . '::update_item'}($id,$data);
    $entity->{id} = $id;
    $map->{$id} = $entity;
    return $entity;

}

sub _find_entity {

    my $class = shift;
    my (@params) = @_;
    foreach my $param (@params) {
        return undef if $param =~ /<n>|<i>|<t>/;
    }
    no strict 'refs';
    return &{$class . '::get_item_filtered'}({@params});

}

sub _get_entity_map {
	my $class = shift;
	if (!exists $entity_maps{$class}) {
		$entity_maps{$class} = {};
	}
	return $entity_maps{$class};
}


sub _info {

    my ($message,$debug) = @_;
    if ($debug) {
        processing_debug(threadid(),$message,getlogger(__PACKAGE__));
    } else {
        processing_info(threadid(),$message,getlogger(__PACKAGE__));
    }

}

1;