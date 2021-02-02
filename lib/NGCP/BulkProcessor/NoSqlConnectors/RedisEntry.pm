package NGCP::BulkProcessor::NoSqlConnectors::RedisEntry;
use strict;

## no critic

use Tie::IxHash;

use NGCP::BulkProcessor::NoSqlConnectors::RedisProcessor qw(init_entry);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
  $HASH_TYPE
  $SET_TYPE
  $LIST_TYPE
  $ZSET_TYPE
  $STRING_TYPE
);

#should correspond to the type names for redis SCAN:
our $HASH_TYPE = 'hash';
our $SET_TYPE = 'set';
our $LIST_TYPE = 'list';
our $ZSET_TYPE = 'zset';
our $STRING_TYPE = 'string';

sub new {

    my $base_class = shift;
    my $class = shift;
    my $type = shift;
    my $self = bless {}, $class;
    $type = 'default' unless $type;
    $type = lc($type);
    my $value;
    if ($type eq 'set') {
        $value = {};
    } elsif ($type eq 'list') {
        $value = [];
    } elsif ($type eq 'zset') {
        my %value = ();
        tie(%value, 'Tie::IxHash');
        $value = \%value;
    } elsif ($type eq 'hash') {
        $value = {};
    } else { #($type eq 'string') {
        $type = 'string';
        $value = undef;
    }
    $self->{type} = $type;
    $self->{value} = $value;
    return init_entry($self,@_);

}

sub getvalue {
    my $self = shift;
    #$self->{value} = shift if scalar @_;
    return $self->{value};
}

sub gettype {
    my $self = shift;
    return $self->{type};
}

sub gethash {
    my $self = shift;
    my $fieldvalues;
    if ($self->{type} eq 'set') {
        $fieldvalues = [ sort keys %{$self->{value}} ];
    } elsif ($self->{type} eq 'list') {
        $fieldvalues = $self->{value};
    } elsif ($self->{type} eq 'zset') {
        $fieldvalues = [ keys %{$self->{value}} ];        
    } elsif ($self->{type} eq 'hash') {
        $fieldvalues = [ map { $self->{value}->{$_}; } sort keys %{$self->{value}} ];
    } else { #($type eq 'string') {
        $fieldvalues = [ $self->{value} ];
    }
    return get_rowhash($fieldvalues);
}

1;
