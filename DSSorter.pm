package DSSorter;
use strict;

## no critic

# guarantee stability, regardless of algorithm
use sort 'stable';

use Logging qw(getlogger);
use LogError qw(sortconfigerror);

use Table;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(sort_by_config_ids
                    sort_by_configs);

#my $logger = getlogger(__PACKAGE__);

sub new {

  my $class = shift;
  my $self = {};
  $self->{sortconfig} = Table->new();
  bless($self,$class);
  return $self;

}

sub add_sorting {

    my $self = shift;
    my ($sorting_id,$numeric,$dir,$memberchain) = @_;

    if (defined $memberchain and ref $memberchain eq 'ARRAY') {
        my @fieldnames = @$memberchain;
        if ((scalar @fieldnames) > 0) {
            $self->{sortconfig}->addrow_nodupe($numeric,$dir,@fieldnames);
        }
    } else {
        sortconfigerror($sorting_id,'chain of object members undefined/invalid',getlogger(__PACKAGE__));
    }

}

sub clear_sorting {

    my $self = shift;
    $self->{sortconfig}->clear();

}

sub sort_array {

    my $self = shift;
    my $array_ptr = shift;

    my $sortconfig = $self->{sortconfig};

    my $sorter = sub ($$) {

        my $a = shift;
        my $b = shift;

        my $result = 0;

        for (my $i = 0; $i < $sortconfig->rowcount(); $i++) {

            my $j = 2;
            my $membername = $sortconfig->element($i,$j);
            my $item_a = ($a ? $a->{$membername} : undef);
            my $item_b = ($b ? $b->{$membername} : undef);
            $j++;
            $membername = $sortconfig->element($i,$j);
            while (defined $membername) {

                $item_a = ($item_a ? $item_a->{$membername} : undef);
                $item_b = ($item_b ? $item_b->{$membername} : undef);
                $j++;
                $membername = $sortconfig->element($i,$j);

            }

            $result = ($result or

                            (

                            $sortconfig->element($i,0) ?
                                ($item_a <=> $item_b) : ($item_a cmp $item_b)

                            ) * $sortconfig->element($i,1)

                      );

        }

        return $result;

    };
    my @sorted = ();

    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        @sorted = sort $sorter @$array_ptr;
    }

    return \@sorted;

}

sub sort_by_config_ids {

    my ($array_ptr,$sortings,$sortingconfigurations) = @_;
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {
        if (defined $sortings and ref $sortings eq 'ARRAY') {
            if (defined $sortingconfigurations and
                ref $sortingconfigurations eq 'HASH') {
                my @sorting_ids = @$sortings;
                if ((scalar @sorting_ids) > 0) {
                    my $sorter = DSSorter->new();
                    foreach my $sorting_id (@sorting_ids) {
                        my $sc = $sortingconfigurations->{$sorting_id};
                        if (defined $sc and ref $sc eq 'HASH') {
                            $sorter->add_sorting($sorting_id,
                                                 $sc->{numeric},
                                                 $sc->{dir},
                                                 $sc->{memberchain});
                        } else {
                            sortconfigerror($sorting_id,
                                    'missing/invalid sorting configuration',
                                    getlogger(__PACKAGE__));
                        }
                    }
                    return $sorter->sort_array($array_ptr);
                }
            } else {
                sortconfigerror(undef,
                                'missing/invalid sorting configurations',
                                getlogger(__PACKAGE__));
            }
        }
        return $array_ptr;
    } else {
        return [];
    }

}

sub sort_by_configs {

    my ($array_ptr,$sortingconfigurations) = @_;
    if (defined $array_ptr and ref $array_ptr eq 'ARRAY') {

        if (defined $sortingconfigurations and
            ref $sortingconfigurations eq 'ARRAY') {

            my @scs = @$sortingconfigurations;
            if ((scalar @scs) > 0) {
                my $sorter = DSSorter->new();
                my $sorting_id = -1;
                foreach my $sc (@scs) {
                    #my $sc = $sortingconfigurations->{$sorting_id};
                    if (defined $sc and ref $sc eq 'HASH') {
                        $sorter->add_sorting($sorting_id,
                                             $sc->{numeric},
                                             $sc->{dir},
                                             $sc->{memberchain});
                    } else {
                        sortconfigerror($sorting_id,
                                'invalid sorting configuration',
                                getlogger(__PACKAGE__));
                    }
                    $sorting_id -= 1;
                }
                return $sorter->sort_array($array_ptr);
            }

        }
        return $array_ptr;
    } else {
        return [];
    }

}

1;