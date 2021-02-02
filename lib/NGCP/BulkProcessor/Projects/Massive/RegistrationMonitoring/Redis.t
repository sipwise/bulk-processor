use strict;

use NGCP::BulkProcessor::Globals qw();
use NGCP::BulkProcessor::ConnectorPool qw(destroy_stores);
use NGCP::BulkProcessor::NoSqlConnectors::Redis qw();
use NGCP::BulkProcessor::Redis::Trunk::location::entry qw(
    get_entry
    get_entry_by_ruid
    process_keys
);

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

SKIP:
{
    $NGCP::BulkProcessor::Globals::location_host = '192.168.0.146';
    my $static_context = {
      
    };
    my $result = process_keys(
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            return 1;
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

SKIP:
{
    my $location = get_entry_by_ruid();
}

exit;

