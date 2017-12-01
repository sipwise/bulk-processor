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
    unless (_load_provider($provider,$reseller_name,$domain_name)) {
        $provider->{contact} = _create_systemcontact();
        $provider->{contract} = _create_contract(
            contact_id => $provider->{contact}->{id},
            billing_profile_id => 1, #default profile id
            type => $type // 'reseller',
        );
        $provider->{reseller} = _create_reseller(
            contract_id => $provider->{contract}->{id},
            name => $reseller_name, #"test <t> <n>",
        );
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
        }

        $provider->{domain} = _create_domain(
            reseller_id => $provider->{reseller}->{id},
            #domain => $domain_name.'.<t>',
            domain => $domain_name,
        );
        $provider->{subscriber_fees} = [];
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
    }
	return $provider;
}

sub _load_provider {
	my ($provider,$reseller_name,$domain_name) = @_;

    my $result = 1;
    $provider->{domain} = _find_entity('NGCP::BulkProcessor::RestRequests::Trunk::Domains',
		domain => $domain_name,
	) if $result;
    $result &= $provider->{domain};
    $provider->{reseller} = _find_entity('NGCP::BulkProcessor::RestRequests::Trunk::Reseller',
		name => $reseller_name,
	) if $result;
    $result &= $provider->{reseller};

    return 0;

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
	my $zone = _create_billing_zone(
		billing_profile_id => $profile->{id},
	);
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

sub _get_domain {

    return _find_entity('NGCP::BulkProcessor::RestRequests::Trunk::Domains',
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

    return _create_entity('NGCP::BulkProcessor::RestRequests::Trunk::BillingFees',
		@_,
	);

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

1;