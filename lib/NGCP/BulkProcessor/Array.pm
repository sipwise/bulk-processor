package NGCP::BulkProcessor::Array;
use strict;

## no critic

use List::Util qw(any uniq);

use NGCP::BulkProcessor::Table;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    mergearrays
    removeduplicates
    itemcount
    grouparray
    reversearray
    contains
    arrayeq
    mapeq
    seteq
    setcontains
    filter
    getroundrobinitem
    getrandomitem
    array_to_map);

sub mergearrays {
    my ($array_ptr1, $array_ptr2) = @_;

    my @result;
    if (defined $array_ptr1 and ref $array_ptr1 eq 'ARRAY') {
        push @result, @{$array_ptr1};
    }
    if (defined $array_ptr2 and ref $array_ptr2 eq 'ARRAY') {
        push @result, @{$array_ptr2};
    }

    return \@result;
}

sub removeduplicates {
    my ($array_ptr, $case_insensitive) = @_;

    my @result;
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        if ($case_insensitive) {
            @result = map { lc } @{$array_ptr};
        } else {
            @result = @{$array_ptr};
        }
        @result = uniq @result;
    }

    return \@result;

}

sub itemcount {
    my ($item, $array_ptr, $case_insensitive) = @_;

    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        if ($case_insensitive) {
            my $lc_item = lc $item;
            return scalar grep { lc eq $lc_item } @{$array_ptr};
        } else {
            return scalar grep { $_ eq $item } @{$array_ptr};
        }
    }

    return 0;
}

sub grouparray {

  my ($array_ptr,$case_insensitive) = @_;
  my $result = NGCP::BulkProcessor::Table->new();
  my $reducedarray = removeduplicates($array_ptr,$case_insensitive);
  my $sort_occurencecount_desc;
  if ($case_insensitive) {
    $sort_occurencecount_desc = sub {

      return ((lc($NGCP::BulkProcessor::Table::b->[1]) <=> lc($NGCP::BulkProcessor::Table::a->[1])) or (lc($NGCP::BulkProcessor::Table::a->[0]) cmp lc($NGCP::BulkProcessor::Table::b->[0])));

    };
  } else {
    $sort_occurencecount_desc = sub {

      return (($NGCP::BulkProcessor::Table::b->[1] <=> $NGCP::BulkProcessor::Table::a->[1]) or ($NGCP::BulkProcessor::Table::a->[0] cmp $NGCP::BulkProcessor::Table::b->[0]));

    };
  }
  foreach my $element (@$reducedarray) {
    $result->addrow_ref([$element,itemcount($element,$array_ptr,$case_insensitive)]);
  }
  $result->sortrows($sort_occurencecount_desc);
  return $result;

}

sub reversearray {
    my ($array_ptr) = @_;

    my @result;
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        @result = reverse @{$array_ptr};
    }

    return \@result;
}

sub contains {
    my ($item, $array_ptr, $case_insensitive) = @_;

    my $result = 0;
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        if ($case_insensitive) {
            my $lc_item = lc $item;
            $result = any { lc eq $lc_item } @{$array_ptr};
        } else {
            $result = any { $_ eq $item } @{$array_ptr};
        }
    }

    return int $result;
}

sub _array_last {
    my $array_ref = shift;

    if (defined $array_ref and ref $array_ref eq 'ARRAY') {
        return $#{$array_ref};
    } else {
        return;
    }
}

sub arrayeq {
  my ($array_ptr1,$array_ptr2,$case_insensitive) = @_;

    my $ubound1 = _array_last($array_ptr1) // -1;
    my $ubound2 = _array_last($array_ptr2) // -1;

  if ($ubound1 != $ubound2) {
    return 0;
  } else {
    if ($case_insensitive) {
      for (my $i = 0; $i <= $ubound1; $i += 1) {
        if (lc($array_ptr1->[$i]) ne lc($array_ptr2->[$i])) {
          return 0;
        }
      }
    } else {
      for (my $i = 0; $i <= $ubound1; $i += 1) {
        if ($array_ptr1->[$i] ne $array_ptr2->[$i]) {
          return 0;
        }
      }
    }
  }

  return 1;

}

sub seteq {

  my ($array_ptr1,$array_ptr2,$case_insensitive) = @_;

    my $ubound1 = _array_last($array_ptr1) // -1;
    my $ubound2 = _array_last($array_ptr2) // -1;

  # every element of array1 must be existent in array2 ...
  for (my $i = 0; $i <= $ubound1; $i += 1) {
    if (not contains($array_ptr1->[$i],$array_ptr2,$case_insensitive)) {
      return 0;
    }
  }
  # ... and every element of array2 must be existent in array1
  for (my $i = 0; $i <= $ubound2; $i += 1) {
    if (not contains($array_ptr2->[$i],$array_ptr1,$case_insensitive)) {
      return 0;
    }
  }

  return 1;

}

sub setcontains {

  my ($array_ptr1,$array_ptr2,$case_insensitive) = @_;

    my $ubound1 = _array_last($array_ptr1) // -1;

  # every element of array1 must be existent in array2:
  for (my $i = 0; $i <= $ubound1; $i += 1) {
    if (not contains($array_ptr1->[$i],$array_ptr2,$case_insensitive)) {
      return 0;
    }
  }

  return 1;

}

sub filter {

  my ($array_ptr1,$array_ptr2,$case_insensitive) = @_;
    my $ubound1 = _array_last($array_ptr1);
    my $ubound2 = _array_last($array_ptr2);

    return [] if not defined $ubound1;
    return $array_ptr1 if not defined $ubound2;

  my @result = ();
  # every element of array1 must be existent in array2 ...
  for (my $i = 0; $i <= $ubound1; $i += 1) {
    if (contains($array_ptr1->[$i],$array_ptr2,$case_insensitive)) {
      push @result,$array_ptr1->[$i];
    }
  }

  return \@result;

}

sub getroundrobinitem {

  my ($array_ptr,$recentindex) = @_;
  if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
    my $size = (scalar @$array_ptr);
    if ($size == 1) {
      return (@{$array_ptr}[0],0);
    } elsif ($size > 1) {
      if (!defined $recentindex or $recentindex < 0) {
        $recentindex = -1;
      }
      my $newindex = ($recentindex + 1) % $size;
      return (@{$array_ptr}[$newindex],$newindex);
    }
  }
  return (undef,undef);

}

sub getrandomitem {

  my ($array_ptr) = @_;
  if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
    my $size = (scalar @$array_ptr);
    if ($size == 1) {
      return (@{$array_ptr}[0],0);
    } elsif ($size > 1) {
      my $newindex = int(rand($size));
      return (@{$array_ptr}[$newindex],$newindex);
    }
  }
  return (undef,undef);

}

sub array_to_map {

    my ($array_ptr,$get_key_code,$get_value_code,$mode) = @_;
    my $map = {};
    my @keys = ();
    my @values = ();
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
      if (defined $get_key_code and ref $get_key_code eq 'CODE') {
        if (not (defined $get_value_code and ref $get_value_code eq 'CODE')) {
          $get_value_code = sub { return shift; };
        }
        $mode = lc($mode);
        if (not ($mode eq 'group' or $mode eq 'first' or $mode eq 'last')) {
          $mode = 'group';
        }
        foreach my $item (@$array_ptr) {
          my $key = &$get_key_code($item);
          if (defined $key) {
            my $value = &$get_value_code($item);
            if (defined $value) {
              if (not exists $map->{$key}) {
                  if ($mode eq 'group') {
                      $map->{$key} = [ $value ];
                  } else {
                      $map->{$key} = $value;
                  }
                  push(@keys,$key);
              } else {
                  if ($mode eq 'group') {
                      push(@{$map->{$key}}, $value);
                  } elsif ($mode eq 'last') {
                      $map->{$key} = $value;
                  }
              }
              push(@values,$value);
            }
          }
        }
      }
    }
    return ($map,\@keys,\@values);

}

sub _hash_size {
    my $hash_ref = shift;

    if (defined $hash_ref and ref $hash_ref eq 'HASH') {
        return scalar keys %{$hash_ref};
    } else {
        return 0;
    }
}


sub mapeq {
  my ($map_prt1,$map_prt2,$case_insensitive) = @_;
    my $key_count1 = _hash_size($map_prt1);
    my $key_count2 = _hash_size($map_prt2);

  if ($key_count1 != $key_count2) {
    return 0; #print "they don't have the same number of keys\n";
  } else {
      my %cmp = map { $_ => 1 } keys %$map_prt1;
      if ($case_insensitive) {
        for my $key (keys %$map_prt2) {
            last unless exists $cmp{$key};
            last unless lc($map_prt1->{$key}) eq lc($map_prt2->{$key});
            delete $cmp{$key};
        }
      } else {
        for my $key (keys %$map_prt2) {
            last unless exists $cmp{$key};
            last unless $map_prt1->{$key} eq $map_prt2->{$key};
            delete $cmp{$key};
        }
      }
      if (%cmp) {
          return 0; #print "they don't have the same keys or values\n";
      } else {
          return 1; #print "they have the same keys or values\n";
      }
  }
}

1;
