use strict;

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::ConnectorPool qw(destroy_stores);
use NGCP::BulkProcessor::NoSqlConnectors::Redis qw();
use NGCP::BulkProcessor::Redis::Trunk::location::entry qw(
    get_entry
    get_entry_by_ruid
    
);
use NGCP::BulkProcessor::Redis::Trunk::location::usrdom qw(
    get_usrdom
    get_usrdom_by_username_domain

);

$NGCP::BulkProcessor::Globals::location_host = '192.168.0.146';

goto SKIP;
{
    my $host = '192.168.0.146';
    my $port = '6379';
    my $sock = undef;
    my $password = undef;
    my $databaseindex = '0';
    
    my $store = NGCP::BulkProcessor::NoSqlConnectors::Redis->new(undef);
    $store->connect(20,undef,$host);
    
    my @result = $store->keys_shared('*',sub {
        my ($reply, $error) = @_;
        die "Oops, got an error: $error\n" if defined $error;
        print "$_\n" for @$reply;
    });
    #print join("\n",@keys);
}

#SKIP:
{
    
    my $static_context = {
      
    };
    my $result = NGCP::BulkProcessor::Redis::Trunk::location::entry::process_keys(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            return 0;
        },
        static_context => $static_context,
        blocksize => 10000,
        init_process_context_code => sub {
            my ($context)= @_;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            destroy_stores();
        },
        multithreading => 1,
        numofthreads => 4,
        #load_recursive => ,
    );
    
}

#SKIP:
{
    my $location = get_entry_by_ruid("x uloc-1-6007e9b2-302b-ce673");
    $location = get_entry("x location:entry::uloc-1-6007e9b2-302b-ce673");
    $location = get_entry_by_ruid("uloc-1-6007e9b2-302b-ce673");
    $location = get_entry("location:entry::uloc-1-6007e9b2-302b-ce673");
}

SKIP:
{
    
    my $static_context = {
      
    };
    my $result = NGCP::BulkProcessor::Redis::Trunk::location::usrdom::process_keys(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            #die();
            print @$records . " done\n";
            return 0;
        },
        static_context => $static_context,
        blocksize => 10000,
        init_process_context_code => sub {
            my ($context)= @_;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            destroy_stores();
        },
        multithreading => 1,
        numofthreads => 4,
        load_recursive => { _entries => 1, },
    );
    
}

#destroy_stores();
exit;

