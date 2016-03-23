# gearman service layer test

use strict;

## no critic

use LoadCLIConfig;

use Logging;

use Test::Unit::Procedural;

use Service::TestService;
use ServiceProxy qw(new_async_do);

use Serialization qw(
    $format_xml
    $format_yaml
    $format_json
    $format_php
    $format_perl
);

my $service1;
my $service2;
my $service3;

    my $service = test::TestService->new();
    my $proxy = ServiceProxy->new();
    #$service1 = test::TestService->new();
    #$service2 = test::TestService->new();
    #$service3 = test::TestService->new();


sub set_up {

    #set_project(yearmonth2projecttag($download_year,$download_month));
#    $service = test::TestService->new();
#    $proxy = ServiceProxy->new();

}


sub test_roundtrip_do {

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }

}

sub test_roundtrip_json {
    $service->stop();
    $service->{serialization_format} = $format_json;
    $proxy->{serialization_format} = $format_json;
    $service->start();

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }
    $service->stop();
    $service->{serialization_format} = undef;
    $proxy->{serialization_format} = undef;
    $service->start();
}

sub test_roundtrip_yaml {
    $service->stop();
    $service->{serialization_format} = $format_yaml;
    $proxy->{serialization_format} = $format_yaml;
    $service->start();

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }
    $service->stop();
    $service->{serialization_format} = undef;
    $proxy->{serialization_format} = undef;
    $service->start();
}

sub Xtest_roundtrip_php {
    $service->stop();
    $service->{serialization_format} = $format_php;
    $proxy->{serialization_format} = $format_php;
    $service->start();

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }
    $service->stop();
    $service->{serialization_format} = undef;
    $proxy->{serialization_format} = undef;
    $service->start();
}

sub test_roundtrip_xml {
    $service->stop();
    $service->{serialization_format} = $format_xml;
    $proxy->{serialization_format} = $format_xml;
    $service->start();

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }
    $service->stop();
    $service->{serialization_format} = undef;
    $proxy->{serialization_format} = undef;
    $service->start();
}

sub test_roundtrip_perl {
    $service->stop();
    $service->{serialization_format} = $format_perl;
    $proxy->{serialization_format} = $format_perl;
    $service->start();

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('roundtrip',\&on_error,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do roundtrip failed');
    }
    $service->stop();
    $service->{serialization_format} = undef;
    $proxy->{serialization_format} = undef;
    $service->start();
}

sub test_noop_do {

    #my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        #$data->{$i} = 'roundtrip test ' . $i;
        #my $input = [ $data, $i ];
        my $output = $proxy->do('noop',\&on_error);
        #print $output->[0]->{$output->[1]} . "\n";
        assert(!defined $output,'service do noop failed');
    }

}

sub test_exception_do {

    #my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        #$data->{$i} = 'roundtrip test ' . $i;
        #my $input = [ $data, $i ];
        my $exception = undef;
        my $output = $proxy->do('exception',sub { $exception = shift; });
        #print $output->[0]->{$output->[1]} . "\n";
        assert(length($exception) > 0,'service do exception failed');
    }

}

sub test_sleep_roundtrip_do {

    my $proxy = ServiceProxy->new(undef,1.5);

    my $data = {};

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'sleep roundtrip test ' . $i;
        my $input = [ $data, $i ];
        my $output = $proxy->do('sleeproundtrip',\&on_error,1,$input);
        print $output->[0]->{$output->[1]} . "\n";
        assert($output->[0]->{$output->[1]} eq $data->{$i},'service do sleep roundtrip failed');
    }

}

sub test_sleep_roundtrip_do_async1 {

    #my $service = test::TestService->new();
    my $proxy = ServiceProxy->new();

    my $data = {};
    my $output = undef;

    for (my $i = 0; $i < 3; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        if ($i > 0) {
            $proxy->wait(3);
            print $output->[0]->{$output->[1]} . "\n";
            assert($output->[0]->{$output->[1]} eq $data->{$i - 1},'service do async roundtrip failed');
            $output = undef;
        }
        assert($proxy->do_async('sleeproundtrip',sub { $output = shift; },\&on_error,2,$input),'service do async failed');

    }
    #$proxy->wait();

}

sub test_sleep_roundtrip_do_async2 {

    my $data = {};
    my @proxies = ();

    my $on_complete = sub { my $output = shift;
                                                       print $output->[0]->{$output->[1]} . "\n";
                                                       #assert($output->[0]->{$output->[1]} eq $data->{$i},'service do async roundtrip failed');
                                                       };

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        #if ($i > 0) {
        #    $proxy->wait();
        #    print $output->[0]->{$output->[1]} . "\n";
            #assert($output->[0]->{$output->[1]} eq $data->{$i - 1},'service do async roundtrip failed');
        #    $output = undef;
        #}
        #my $proxy;
        #if ($i % 3 == 0) {
        #   $proxy = $proxy1;
        #} elsif ($i % 3 == 1) {
        #   $proxy = $proxy2;
        #} elsif ($i % 3 == 1) {
        #    $proxy = $proxy3;
        #}

        my $proxy = new_async_do('sleeproundtrip', $on_complete, \&on_error, 0, $input); #,
                                                  #sub {
                                                        #print shift . "\n";
                                                        ##assert(0,'on_error: ' . shift);
                                                        #},
                                                        #0,$input);
        assert(defined $proxy,'proxy not created');
        #$proxy->wait();
        push(@proxies, $proxy);

    }



    #undef @proxies;
    #$service->stop();
    #$proxy->wait();

}

sub test_exception_do_async {

    my $data = {};
    my @proxies = ();

    my $on_error = sub { my $exception = shift;
                                                        print $exception . "\n";
                                                       assert(length($exception) > 0,'service do async roundtrip failed');
                                                       };

    for (my $i = 0; $i < 10; $i++) {
        $data->{$i} = 'roundtrip test ' . $i;
        my $input = [ $data, $i ];
        #if ($i > 0) {
        #    $proxy->wait();
        #    print $output->[0]->{$output->[1]} . "\n";
            #assert($output->[0]->{$output->[1]} eq $data->{$i - 1},'service do async roundtrip failed');
        #    $output = undef;
        #}
        #my $proxy;
        #if ($i % 3 == 0) {
        #   $proxy = $proxy1;
        #} elsif ($i % 3 == 1) {
        #   $proxy = $proxy2;
        #} elsif ($i % 3 == 1) {
        #    $proxy = $proxy3;
        #}

        my $proxy = new_async_do('sleeproundtrip', undef, $on_error, 0, $input); #,
                                                  #sub {
                                                        #print shift . "\n";
                                                        ##assert(0,'on_error: ' . shift);
                                                        #},
                                                        #0,$input);
        assert(defined $proxy,'proxy not created');
        #$proxy->wait();
        push(@proxies, $proxy);

    }



    #undef @proxies;
    #$service->stop();
    #$proxy->wait();

}

sub on_error {
    print shift . "\n";
    #assert(0,'on_error: ' . shift);
}

sub tear_down {

#undef $service;
#undef $proxy;

}

create_suite();
run_suite();

#destroy_dbs();
undef $service;
undef $proxy;

exit;