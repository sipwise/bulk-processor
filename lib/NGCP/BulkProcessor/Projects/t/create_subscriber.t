use Net::Domain qw(hostfqdn);
use JSON -support_by_pp, -no_export;
use Test::More;
use Data::Dumper;
use LWP::UserAgent;

my $is_local_env = 1;

my $uri = $ENV{CATALYST_SERVER} || ('https://'.hostfqdn.':4443');

my ($ua, $req, $res);

my ($netloc) = ($uri =~ m!^https?://(.*)/?.*$!);

$ua = LWP::UserAgent->new;

$ua->ssl_opts(
        verify_hostname => 0,
        SSL_verify_mode => 0,
    );
my $user = $ENV{API_USER} // 'administrator';
my $pass = $ENV{API_PASS} // 'administrator';
$ua->credentials($netloc, "api_admin_http", $user, $pass);

#$ua->add_handler("request_send",  sub {
#    my ($request, $ua, $h) = @_;
#    print $request->method . ' ' . $request->uri . "\n" . ($request->content ? $request->content . "\n" : '') unless $request->header('authorization');
#    return undef;
#});
#$ua->add_handler("response_done", sub {
#    my ($response, $ua, $h) = @_;
#    print $response->decoded_content . "\n" if $response->code != 401;
#    return undef;
#});

my $t = time;
my $reseller_id = 1;

$req = HTTP::Request->new('POST', $uri.'/api/domains/');
$req->header('Content-Type' => 'application/json');
$req->content(JSON::to_json({
    domain => 'test' . $t . '.example.org',
    reseller_id => $reseller_id,
}));
$res = $ua->request($req);
is($res->code, 201, "create test domain");
$req = HTTP::Request->new('GET', $uri.'/'.$res->header('Location'));
$res = $ua->request($req);
is($res->code, 200, "fetch created test domain");
my $domain = JSON::from_json($res->decoded_content);

$req = HTTP::Request->new('POST', $uri.'/api/billingprofiles/');
$req->header('Content-Type' => 'application/json');
$req->header('Prefer' => 'return=representation');
$req->content(JSON::to_json({
    name => "test profile $t",
    handle  => "testprofile$t",
    reseller_id => $reseller_id,
}));
$res = $ua->request($req);
is($res->code, 201, "create test billing profile");
my $billing_profile_id = $res->header('Location');
$billing_profile_id =~ s/^.+\/(\d+)$/$1/;

$req = HTTP::Request->new('POST', $uri.'/api/customercontacts/');
$req->header('Content-Type' => 'application/json');
$req->content(JSON::to_json({
    firstname => "cust_contact_first",
    lastname  => "cust_contact_last",
    email     => "cust_contact\@custcontact.invalid",
    reseller_id => $reseller_id,
}));
$res = $ua->request($req);
is($res->code, 201, "create test customer contact");
$req = HTTP::Request->new('GET', $uri.'/'.$res->header('Location'));
$res = $ua->request($req);
is($res->code, 200, "fetch test customer contact");
my $custcontact = JSON::from_json($res->decoded_content);

my %subscriber_map = ();
my %customer_map = ();

#goto SKIP;
{ #end_ivr:
    my $customer = _create_customer(
        type => "sipaccount",
        );
    my $cc = 800;
    my $ac = '1'.(scalar keys %subscriber_map);
    my $sn = $t;
    my $aliases = [
        { cc => $cc, ac => $ac, sn => $sn.'01' },
        { cc => $cc, ac => $ac, sn => $sn.'02' },
    ];
    my $subscriber = _create_subscriber($customer,
        primary_number => { cc => $cc, ac => $ac, sn => $sn },
        alias_numbers => $aliases,
    );

    #my $call_forwards = set_callforwards($subscriber,{ cfu => {
    #            destinations => [
    #                { destination => "5678" },
    #                { destination => "autoattendant", },
    #            ],
    #        }});
    #$call_forwards = set_callforwards($subscriber,{ cfu => {
    #            destinations => [
    #                { destination => "5678" },
    #            ],
    #        }});
    print "blah";
}


sub _create_cfmapping {
    my ($subscriber,$mappings) = @_;

    my $cfmapping_uri = $uri.'/api/cfmappings/'.$subscriber->{id};
    $req = HTTP::Request->new('PUT', $cfmapping_uri); #$customer->{id});
    $req->header('Content-Type' => 'application/json');
    $req->header('Prefer' => 'return=representation');
    $req->content(JSON::to_json($mappings));
    $res = $ua->request($req);
    is($res->code, 200, "create test cfmappings");
    $req = HTTP::Request->new('GET', $cfmapping_uri); # . '?page=1&rows=' . (scalar keys %$put_data));
    $res = $ua->request($req);
    is($res->code, 200, "fetch test cfmappings");
    return JSON::from_json($res->decoded_content);
}

sub _update_cfmapping {
    my ($subscriber,$cf_type,$mapping) = @_;
    my $cfmapping_uri = $uri.'/api/cfmappings/'.$subscriber->{id};
    $req = HTTP::Request->new('PATCH', $cfmapping_uri);
    $req->header('Content-Type' => 'application/json-patch+json');
    $req->header('Prefer' => 'return=representation');
    $req->content(JSON::to_json(
        [ { op => 'replace', path => '/'.$cf_type, value => $mapping } ]
    ));
    $res = $ua->request($req);
    is($res->code, 200, "update test cfmappings");
    $req = HTTP::Request->new('GET', $cfmapping_uri);
    $res = $ua->request($req);
    is($res->code, 200, "fetch updated test cfmappings");
    return JSON::from_json($res->decoded_content);
}

sub _create_cftimeset {
    my ($subscriber,$times) = @_;

    $req = HTTP::Request->new('POST', $uri.'/api/cftimesets/');
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::to_json({
        name => "cf_time_set_".$t,
        subscriber_id => $subscriber->{id},
        times => $times,
    }));
    $res = $ua->request($req);
    is($res->code, 201, "create test cftimeset");
    my $cftimeset_uri = $uri.'/'.$res->header('Location');
    $req = HTTP::Request->new('GET', $cftimeset_uri);
    $res = $ua->request($req);
    is($res->code, 200, "fetch created test cftimeset");
    return JSON::from_json($res->decoded_content);

}

sub _create_cfdestinationset {
    my ($subscriber,$name,$destinations) = @_;

    $req = HTTP::Request->new('POST', $uri.'/api/cfdestinationsets/');
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::to_json({
        name => $name,
        subscriber_id => $subscriber->{id},
        destinations => $destinations,
    }));
    $res = $ua->request($req);
    is($res->code, 201, "create test cfdestinationset");
    my $cfdestinationset_uri = $uri.'/'.$res->header('Location');
    $req = HTTP::Request->new('GET', $cfdestinationset_uri);
    $res = $ua->request($req);
    is($res->code, 200, "fetch created test cfdestinationset");
    return JSON::from_json($res->decoded_content);

}

sub _update_cfdestinationset {
    my ($destinationset,$destinations) = @_;
    my $cfdestinationset_uri = $uri.'/api/cfdestinationsets/'.$destinationset->{id};
    $req = HTTP::Request->new('PATCH', $cfdestinationset_uri);
    $req->header('Content-Type' => 'application/json-patch+json');
    $req->header('Prefer' => 'return=representation');
    $req->content(JSON::to_json(
        [ { op => 'replace', path => '/destinations', value => $destinations } ]
    ));
    $res = $ua->request($req);
    is($res->code, 200, "update test cfdestinationset");
    $req = HTTP::Request->new('GET', $cfdestinationset_uri);
    $res = $ua->request($req);
    is($res->code, 200, "fetch updated test cfdestinationset");
    return JSON::from_json($res->decoded_content);

}

sub set_callforwards {
    my ($subscriber,$call_forwards) = @_;

    my $callforward_uri = $uri.'/api/callforwards/'.$subscriber->{id};
    $req = HTTP::Request->new('PUT', $callforward_uri); #$customer->{id});
    $req->header('Content-Type' => 'application/json');
    $req->header('Prefer' => 'return=representation');
    $req->content(JSON::to_json($call_forwards));
    $res = $ua->request($req);
    is($res->code, 200, "set test callforwards");
    $req = HTTP::Request->new('GET', $callforward_uri); # . '?page=1&rows=' . (scalar keys %$put_data));
    $res = $ua->request($req);
    is($res->code, 200, "fetch test callforwards");
    return JSON::from_json($res->decoded_content);

}

sub _get_subscriber {

    my ($subscriber) = @_;
    $req = HTTP::Request->new('GET', $uri.'/api/subscribers/'.$subscriber->{id});
    $res = $ua->request($req);
    is($res->code, 200, "fetch test subscriber");
    $subscriber = JSON::from_json($res->decoded_content);
    $subscriber_map{$subscriber->{id}} = $subscriber;
    return $subscriber;

}

sub _create_subscriber {

    my ($customer,@further_opts) = @_;
    $req = HTTP::Request->new('POST', $uri.'/api/subscribers/');
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::to_json({
        domain_id => $domain->{id},
        username => 'subscriber_' . (scalar keys %subscriber_map) . '_'.$t,
        password => 'subscriber_password',
        customer_id => $customer->{id},
        #status => "active",
        @further_opts,
    }));
    $res = $ua->request($req);
    is($res->code, 201, "create test subscriber");
    $req = HTTP::Request->new('GET', $uri.'/'.$res->header('Location'));
    $res = $ua->request($req);
    is($res->code, 200, "fetch test subscriber");
    my $subscriber = JSON::from_json($res->decoded_content);
    $subscriber_map{$subscriber->{id}} = $subscriber;
    return $subscriber;

}

sub _update_subscriber {

    my ($subscriber,@further_opts) = @_;
    $req = HTTP::Request->new('PUT', $uri.'/api/subscribers/'.$subscriber->{id});
    $req->header('Content-Type' => 'application/json');
    $req->header('Prefer' => 'return=representation');
    $req->content(JSON::to_json({
        %$subscriber,
        @further_opts,
    }));
    $res = $ua->request($req);
    is($res->code, 200, "update test subscriber");
    $subscriber = JSON::from_json($res->decoded_content);
    $subscriber_map{$subscriber->{id}} = $subscriber;
    return $subscriber;

}

sub _create_customer {

    my (@further_opts) = @_;
    $req = HTTP::Request->new('POST', $uri.'/api/customers/');
    $req->header('Content-Type' => 'application/json');
    $req->content(JSON::to_json({
        status => "active",
        contact_id => $custcontact->{id},
        type => "sipaccount",
        billing_profile_id => $billing_profile_id,
        max_subscribers => undef,
        external_id => undef,
        #status => "active",
        @further_opts,
    }));
    $res = $ua->request($req);
    is($res->code, 201, "create test customer");
    $req = HTTP::Request->new('GET', $uri.'/'.$res->header('Location'));
    $res = $ua->request($req);
    is($res->code, 200, "fetch test customer");
    my $customer = JSON::from_json($res->decoded_content);
    $customer_map{$customer->{id}} = $customer;
    return $customer;

}


sub _req_to_debug {
    my $request = shift;
    return { request => $request->method . " " . $request->uri,
            headers => $request->headers };
}

sub _get_query_string {
    my ($filters) = @_;
    my $query = '';
    foreach my $param (keys %$filters) {
        if (length($query) == 0) {
            $query .= '?';
        } else {
            $query .= '&';
        }
        $query .= $param . '=' . $filters->{$param};
    }
    return $query;
};

done_testing;
