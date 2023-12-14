package NGCP::BulkProcessor::NoSqlConnectors::RedisEntry;
use strict;

## no critic

use Tie::IxHash;

use NGCP::BulkProcessor::Utils qw(load_module);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
  copy_value
  
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
    $self->{key} = shift;
    $type = '' unless $type;
    $type = lc($type);
    my $value;
    if ($type eq $SET_TYPE) {
        # a redis "set" is a perl hash with undetermined values. 
        $value = {};
    } elsif ($type eq $LIST_TYPE) {
        # a redis "list" is an perl array.
        $value = [];
    } elsif ($type eq $ZSET_TYPE) {
        # a redis "zset" is a perl hash with ordered keys and undetermined values. 
        my %value = ();
        tie(%value, 'Tie::IxHash');
        $value = \%value;
    } elsif ($type eq $HASH_TYPE) {
        # a redis "hash" is a perl hash.
        $value = {};
    } else {
        # a redis "string" is a perl scalar.
        $type = $STRING_TYPE;
        $value = undef;
    }
    $self->{type} = $type;
    $self->{value} = $value;
    return _init_entry($self,@_);

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

sub getkey {
    my $self = shift;
    return $self->{key};
}

sub gethash {
    my $self = shift;
    my $fieldvalues;
    if ($self->{type} eq $SET_TYPE) {
        $fieldvalues = [ sort keys %{$self->{value}} ];
    } elsif ($self->{type} eq $LIST_TYPE) {
        $fieldvalues = $self->{value};
    } elsif ($self->{type} eq $ZSET_TYPE) {
        $fieldvalues = [ keys %{$self->{value}} ];        
    } elsif ($self->{type} eq $HASH_TYPE) {
        $fieldvalues = [ map { $self->{value}->{$_}; } sort keys %{$self->{value}} ];
    } else { #($type eq 'string') {
        $fieldvalues = [ $self->{value} ];
    }
    return get_rowhash($fieldvalues);
}

sub _init_entry {

    my ($entry,$fieldnames) = @_;
    
    if (defined $fieldnames) {
        # if there are fieldnames defined, we make a member variable for each and set it to undef
        foreach my $fieldname (@$fieldnames) {
            $entry->{value}->{$fieldname} = undef;
        }
    }

    return $entry;

}

sub copy_value {
    my ($entry,$value,$fieldnames) = @_;
    if (defined $entry) {
        if (defined $value) {
            if ($entry->{type} eq $SET_TYPE) {
                if (ref $value eq 'ARRAY') {
                    %{$entry->{value}} = map { $_ => undef; } @$value;
                } elsif (ref $value eq 'HASH') {
                    %{$entry->{value}} = map { $_ => undef; } %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    %{$entry->{value}} = %{$value->{value}};
                } else {
                    $entry->{value} = { $value => undef, };
                }
            } elsif ($entry->{type} eq $LIST_TYPE) {
                if (ref $value eq 'ARRAY') {
                    @{$entry->{value}} = @$value;
                } elsif (ref $value eq 'HASH') {
                    @{$entry->{value}} = %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    @{$entry->{value}} = @{$value->{value}};
                } else {
                    $entry->{value} = [ $value, ];
                }                
            } elsif ($entry->{type} eq $ZSET_TYPE) {
                my %value = ();
                tie(%value, 'Tie::IxHash');
                $entry->{value} = \%value;
                if (ref $value eq 'ARRAY') {
                    map { $entry->{value}->Push($_ => undef); } @$value;
                } elsif (ref $value eq 'HASH') {
                    map { $entry->{value}->Push($_ => undef); } %$value;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    map { $entry->{value}->Push($_ => undef); } keys %{$value->{value}};
                } else {
                    $entry->{value}->Push($value => undef);
                }
            } elsif ($entry->{type} eq $HASH_TYPE) {
                my $i;
                if (ref $value eq 'ARRAY') {
                    $i = 0;
                } elsif (ref $value eq 'HASH') {
                    $i = -1;
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    $i = -2;
                } else {
                    $i = -3;
                }
                foreach my $fieldname (@$fieldnames) {
                    if ($i >= 0) {
                        $entry->{value}->{$fieldname} = $value->[$i];
                        $i++;
                    } elsif ($i == -1) {
                        if (exists $value->{$fieldname}) {
                            $entry->{value}->{$fieldname} = $value->{$fieldname};
                        } elsif (exists $value->{uc($fieldname)}) {
                            $entry->{value}->{$fieldname} = $value->{uc($fieldname)};
                        } else {
                            $entry->{value}->{$fieldname} = undef;
                        }
                    } elsif ($i == -2) {
                        if (exists $value->{value}->{$fieldname}) {
                            $entry->{value}->{$fieldname} = $value->{value}->{$fieldname};
                        } elsif (exists $entry->{value}->{uc($fieldname)}) {
                            $entry->{value}->{$fieldname} = $value->{value}->{uc($fieldname)};
                        } else {
                            $entry->{value}->{$fieldname} = undef;
                        }                        
                    } else {
                        $entry->{value}->{$fieldname} = $value; #scalar
                        last;
                    }
                }
            } else { #($type eq 'string') {
                if (ref $value eq 'ARRAY') {
                    $entry->{value} = $value->[0];
                } elsif (ref $value eq 'HASH') {
                    my @keys = keys %$value; #Experimental shift on scalar is now forbidden at..
                    $entry->{value} = $value->{shift @keys};
                } elsif (ref $value eq ref $entry) {
                    die('redis type mismatch') if $entry->{type} ne $value->{type};
                    $entry->{value} = $value->{value};
                } else {
                    $entry->{value} = $value;
                }                
            }
        }

    }
    return $entry;
}

sub load_relation {
    my $self = shift;
    my ($load_recursive,$relation,$findby,@findby_args) = @_;
    if ($load_recursive and 'HASH' eq ref $load_recursive and length($relation)) {
        my $relation_path;
        my $relation_path_backup = $load_recursive->{_relation_path};
        if (length($relation_path_backup)) {
            $relation_path = $relation_path_backup . '.' . $relation;
        } else {
            no strict "refs";  ## no critic (ProhibitNoStrict)
            $relation_path = ((ref $self) . '::gettablename')->() . '.' . $relation;
        }
        my $include = $load_recursive->{$relation_path};
        my $filter;
        my $transform;
        if ('HASH' eq ref $include) {
            $filter = $include->{filter};
            $transform = $include->{transform};
            if (exists $include->{include}) {
                $include = $include->{include};
            } elsif ($transform or $filter) {
                $include = 1;
            } 
        }
        if (('CODE' eq ref $include and $include->($self))
             or (not ref $include and $include)) {
            load_module($findby);
            no strict "refs";  ## no critic (ProhibitNoStrict)
            $load_recursive->{_relation_path} = $relation_path;
            $self->{$relation} = $findby->(@findby_args);
            if ('ARRAY' eq ref $self->{$relation}
                and 'CODE' eq ref $filter) {
                my $closure = _closure($filter,$load_recursive->{_context});
                $self->{$relation} = [ grep { $closure->($_); } @{$self->{$relation}}];
            }
            if ('CODE' eq ref $transform) {
                my $closure = _closure($transform,$load_recursive->{_context});
                $self->{$relation} = $closure->($self->{$relation});
            }
            $load_recursive->{_relation_path} = $relation_path_backup;
            return 1;
        }
    }
    return 0;
}

sub _closure {
    my ($sub,$context) = @_;
    return sub {
        foreach my $key (keys %$context) {
            no strict "refs";  ## no critic (ProhibitNoStrict)
            *{"main::$key"} = $context->{$key} if 'CODE' eq ref $context->{$key};
        }
        return $sub->(@_,$context);
    };
}

1;
