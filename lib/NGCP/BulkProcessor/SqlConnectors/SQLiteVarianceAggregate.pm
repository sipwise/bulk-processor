# helper module to implement variance aggregate function with SQLite
# adamk@cpan.org
# 2009

# used only for testing custom functions ...

package NGCP::BulkProcessor::SqlConnectors::SQLiteVarianceAggregate;
use strict;

## no critic

  #sub new { bless [], shift; }
  #
  #sub step {
  #    my ( $self, $value ) = @_;
  #
  #    push @$self, $value;
  #}
  #
  #sub finalize {
  #    my $self = $_[0];
  #
  #    my $n = @$self;
  #
  #    # Variance is NULL unless there is more than one row
  #    return undef unless $n || $n == 1;
  #
  #    my $mu = 0;
  #    foreach my $v ( @$self ) {
  #        $mu += $v;
  #    }
  #    $mu /= $n;
  #
  #    my $sigma = 0;
  #    foreach my $v ( @$self ) {
  #        #$sigma += ($x - $mu)**2;
  #        $sigma += ($v - $mu)**2;
  #    }
  #    $sigma = $sigma / ($n - 1);
  #
  #    return $sigma;
  #}

my $mu = 0;
my $count = 0;
my $S = 0;

sub new { bless [], shift; }

sub step {
    my ( $self, $value ) = @_;
    $count++;
    my $delta = $value - $mu;
    $mu += $delta / $count;
    $S += $delta * ($value - $mu);
}

sub finalize {
    my $self = $_[0];
    return $S / ($count - 1);
}

1;
