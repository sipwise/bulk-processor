package NGCP::BulkProcessor::Redis::mr755::location::usrdom;
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

use NGCP::BulkProcessor::Redis::mr755::location::entry qw();

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::NoSqlConnectors::RedisEntry);
our @EXPORT_OK = qw(
    get_usrdom
    get_usrdom_by_username_domain
    process_keys
);

my $get_store = \&get_location_store;

my $table = '1:location:usrdom';
my $type = $NGCP::BulkProcessor::NoSqlConnectors::RedisEntry::SET_TYPE;
my $get_key = sub {
    my ($username,$domain) = @_;
    my $result = $table . '::' . $username;
    $result .= ':' . $domain if $domain;
    return $result;
};

sub new {

    my $class = shift;
    my $self = NGCP::BulkProcessor::NoSqlConnectors::RedisEntry->new($class,$type,shift);

    copy_value($self,shift);

    return $self;

}

sub get_usrdom {

    my ($key,$load_recursive) = @_;
    my $store = &$get_store();
    
    if (length($key) and my @res = $store->smembers($key)) {
        return builditems_fromrows($key,\@res,$load_recursive);
    }
    return undef;

}

sub get_usrdom_by_username_domain {

    my ($username,$domain,$load_recursive) = @_;
    my $store = &$get_store();
    
    if ($username and $domain and my @res = $store->smembers(my $key = &$get_key($username,$domain))) {
        return builditems_fromrows($key,\@res,$load_recursive);
    }
    return undef;

}

sub builditems_fromrows {

    my ($keys,$rows,$load_recursive) = @_;

    my $item;
    
    if (defined $keys and ref $keys eq 'ARRAY') {
        my @items = ();
        foreach my $key (@$keys) {
            $item = __PACKAGE__->new($key, $rows->[scalar @items]);

            transformitem($item,$load_recursive);

            push @items,$item;

        }
        return \@items;
    } else {
        $item = __PACKAGE__->new($keys,$rows);
        transformitem($item,$load_recursive);
        return $item;
    }

}

sub transformitem {
    my ($item,$load_recursive) = @_;

    # transformations go here ...
    if ($load_recursive) {
        $load_recursive = {} unless ref $load_recursive;
        my $field = "_entries";
        if ($load_recursive->{$field}) {
            my @entries = ();
            foreach my $element (keys %{$item->getvalue()}) {
                push(@entries,NGCP::BulkProcessor::Redis::mr755::location::entry::get_entry($element,$load_recursive));
            }
            $item->{$field} = \@entries;
        }
    }
 
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
                map { [ &$get_store()->smembers($_) ]; } @$rowblock
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

