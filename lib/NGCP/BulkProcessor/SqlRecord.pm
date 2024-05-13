package NGCP::BulkProcessor::SqlRecord;
use strict;

use threads::shared;

## no critic

use NGCP::BulkProcessor::Table qw(get_rowhash);

use NGCP::BulkProcessor::SqlProcessor qw(init_record);

use NGCP::BulkProcessor::Utils qw(load_module);

use NGCP::BulkProcessor::Closure qw(is_code);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw();

sub new {

    my $base_class = shift;
    my $class = shift;
    my $self = bless {}, $class;
    return init_record($self,$class,@_);

}

sub new_shared {

    my $base_class = shift;
    my $class = shift;
    my %obj : shared = ();
    my $self = bless \%obj, $class;
    return init_record($self,$class,@_);

}

sub gethash {
    my $self = shift;
    my @fieldvalues = ();
    foreach my $field (sort keys %$self) { #http://www.perlmonks.org/?node_id=997682
        my $value = $self->{$field};
        if (ref $value eq '') {
            push(@fieldvalues,$value);
        }
    }
    return get_rowhash(\@fieldvalues);
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
            } elsif (exists $include->{load}) {
                $include = $include->{load};                
            } elsif ($transform or $filter) {
                $include = 1;
            } 
        }
        if ((is_code($include) and $self->_calc_closure($relation_path,'load',$include,$load_recursive->{_context},$load_recursive->{_cache},$self))
             or (not ref $include and $include)) {
            load_module($findby);
            no strict "refs";  ## no critic (ProhibitNoStrict)
            $load_recursive->{_relation_path} = $relation_path;
            $self->{$relation} = $findby->(@findby_args);
            if ('ARRAY' eq ref $self->{$relation}
                and is_code($filter)) {
                my $cache = $load_recursive->{_cache} // {};
                $self->{$relation} = [ grep { $self->_calc_closure($relation_path,'filter',$filter,$load_recursive->{_context},$cache,$_,$self); } @{$self->{$relation}} ];
            }
            if (is_code($transform)) {
                $self->{$relation} = $self->_calc_closure($relation_path,'transform',$transform,$load_recursive->{_context},$load_recursive->{_cache},$self->{$relation},$self);
            }
            $load_recursive->{_relation_path} = $relation_path_backup;
            return 1;
        }
    }
    return 0;
}

sub _calc_closure {

    my $self = shift;
    my ($relation_path,$func,$code,$context,$cache,@args) = @_;
    my $id = '_relations_' . $func . '_' . $relation_path;
    $cache //= {};
    $cache->{$id} = NGCP::BulkProcessor::Closure->new($code,$context,"relations '$relation_path' $func'") unless exists $cache->{$id};
    return $cache->{$id}->calc($context,@args);

}

1;
