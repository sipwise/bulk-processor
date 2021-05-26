package NGCP::BulkProcessor::Redis::mr755::location::entry;
use strict;

## no critic

use NGCP::BulkProcessor::ConnectorPool qw(
    get_location_store
    destroy_stores
);

use NGCP::BulkProcessor::NoSqlConnectors::RedisProcessor qw(
    process_entries
);

use NGCP::BulkProcessor::NoSqlConnectors::RedisEntry qw(
    copy_value
);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::NoSqlConnectors::RedisEntry);
our @EXPORT_OK = qw(
    get_entry
    get_entry_by_ruid
    process_keys
);

my $get_store = \&get_location_store;

my $table = '1:location:entry';
my $type = $NGCP::BulkProcessor::NoSqlConnectors::RedisEntry::HASH_TYPE;
my $get_key = sub {
    my ($ruid) = @_;
    return $table . '::' . $ruid;
};

my $fieldnames = [
    'instance',
    'domain',
    'cseq',
    'partition',
    'ruid',
    'connection_id',
    'username',
    'keepalive',
    'path',
    'reg_id',
    'contact',
    'flags',
    'received',
    'callid',
    'socket',
    'cflags',
    'expires',
    'methods',
    'user_agent',
    'q',
    'last_modified',
    'server_id',
];

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::NoSqlConnectors::RedisEntry->new($class,$type,shift,$fieldnames);

    copy_value($self,shift,$fieldnames);

    return $self;

}

sub get_entry {

    my ($key,$load_recursive) = @_;
    my $store = &$get_store();
    
    if (length($key) and my %res = $store->hgetall($key)) {
        return builditems_fromrows($key,\%res,$load_recursive);
    }
    return undef;

}

sub get_entry_by_ruid {

    my ($ruid,$load_recursive) = @_;
    my $store = &$get_store();
    
    if ($ruid and my %res = $store->hgetall(my $key = &$get_key($ruid))) {
        return builditems_fromrows($key,\%res,$load_recursive);
    }
    return undef;

}

sub builditems_fromrows {

    my ($keys,$rows,$load_recursive) = @_;

    my $item;

    if (defined $rows and ref $rows eq 'ARRAY') {
        my @items = ();
        foreach my $row (@$rows) {
            $item = __PACKAGE__->new($keys->[scalar @items], $row);

            # transformations go here ...

            push @items,$item;

        }
        return \@items;
    } elsif (defined $rows and ref $rows eq 'HASH') {
        $item = __PACKAGE__->new($keys,$rows);
        return $item;
    }
    return undef;

}

sub process_keys {

    my %params = @_;
    my ($process_code,
        $static_context,
        $blocksize,
        $init_process_context_code,
        $uninit_process_context_code,
        $multithreading,
        $numofthreads,
        $load_recursive) = @params{qw/
            process_code
            static_context
            blocksize
            init_process_context_code
            uninit_process_context_code
            multithreading
            numofthreads
            load_recursive
        /};

    return process_entries(
        get_store => $get_store,
        scan_pattern => &$get_key('*'),
        type => $type,
        process_code => sub {
            my ($context,$rowblock,$row_offset) = @_;
            return &$process_code($context,builditems_fromrows(\@$rowblock,[
                map { { &$get_store()->hgetall($_) }; } @$rowblock
            ],$load_recursive),$row_offset);
        },
        static_context                  => $static_context,
        blocksize                       => $blocksize,
        init_process_context_code       => $init_process_context_code,
        uninit_process_context_code     => $uninit_process_context_code,
        destroy_reader_stores_code     => \&destroy_stores,
        multithreading                  => $multithreading,
        nosqlprocessing_threads    => $numofthreads,
    );
}

1;

