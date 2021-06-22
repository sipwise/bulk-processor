package NGCP::BulkProcessor::SqlRecord;
use strict;

use threads::shared;

## no critic

use NGCP::BulkProcessor::Table qw(get_rowhash);

use NGCP::BulkProcessor::SqlProcessor qw(init_record);

use NGCP::BulkProcessor::Utils qw(load_module);

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
        my $transfrom;
        if ('HASH' eq ref $include) {
            $filter = $include->{filter};
            $transfrom = $include->{transform};
            if (exists $include->{include}) {
                $include = $include->{include};
            } elsif ($transfrom or $filter) {
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
            if ('CODE' eq ref $transfrom) {
                my $closure = _closure($transfrom,$load_recursive->{_context});
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
