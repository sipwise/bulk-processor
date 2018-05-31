package NGCP::BulkProcessor::Table;
use strict;

## no critic

use Digest::MD5;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(get_rowhash);

sub new {

  my $class = shift;
  my $self = bless {}, $class;
  $self->_set_data($_[0],$_[1]);
  return $self;

}

sub _set_data {

  my $self = shift;
  my ($data,$dupecheck) = shift;
  $self->clear();
  if (defined $data and ref $data eq 'ARRAY') {
    if ($dupecheck) {
      foreach my $row (@$data) {
        $self->addrow_ref_nodupe($row);
      }
    } else {
      foreach my $row (@$data) {
        $self->addrow_ref($row);
      }
    }
  }

}

sub clear {

  my $self = shift;
  $self->{data} = [];
  $self->{rowhashes} = {};

}

sub data_ref {

  my $self = shift;
  if ($_[0]) {
    #if argument, set the value
    $self->_set_data($_[0],$_[1]);
  } else {
    return $self->{data};
  }

}

sub addrow {

  my $self = shift;
  #my @row = @_;
  return $self->addrow_ref(\@_);

}

sub addrow_nodupe {

  my $self = shift;
  #my @row = @_;
  return $self->addrow_ref_nodupe(\@_);

}

sub addrow_ref {

  my $self = shift;
  my $row_ref = shift;
  my $rowhash = get_rowhash($row_ref);
  my $itemcount = 0;
  if (defined $rowhash) {
    if (not exists $self->{rowhashes}->{$rowhash}) {
      $self->{rowhashes}->{$rowhash} = 0;
    }
    $itemcount = $self->{rowhashes}->{$rowhash} + 1;
    $self->{rowhashes}->{$rowhash} = $itemcount;
    push @{$self->{data}},$row_ref;
  }
  return $itemcount;

}

sub addrow_ref_nodupe {

  my $self = shift;
  my $row_ref = shift;
  my $rowhash = get_rowhash($row_ref);
  my $itemcount = 0;
  if (defined $rowhash) {
    if (not exists $self->{rowhashes}->{$rowhash}) {
      $self->{rowhashes}->{$rowhash} = 1;
      $itemcount = 1;
      push @{$self->{data}},$row_ref;
    } else {
      $itemcount = $self->{rowhashes}->{$rowhash};
    }
  }
  return $itemcount;

}

sub rowexists {

  my $self = shift;
  #my @row = @_;
  return $self->rowexists_ref(\@_);

}

sub rowexists_ref {

  my $self = shift;
  my $row_ref = shift;
  my $rowhash = get_rowhash($row_ref);
  my $itemcount = 0;
  if (defined $rowhash) {
    if (exists $self->{rowhashes}->{$rowhash}) {
      return 1;
    }
  }
  return 0;

}

sub get_rowhash {

  my $row_ref = shift;
  if (defined $row_ref and ref $row_ref eq 'ARRAY') {
    my $md5 = Digest::MD5->new;
    foreach my $element (@$row_ref) {
      $md5->add($element);
    }
    return $md5->hexdigest;
  } else {
    return undef;
  }

}

sub rowcount {

  my $self = shift;
  #my @rows = @{$self->{data}};
  return scalar @{$self->{data}}; # + 1;

}

sub element {

  my $self = shift;
  return $self->{data}->[$_[0]]->[$_[1]];

}

sub getrow {

  my $self = shift;
  my $row_ref = $self->{data}->[$_[0]];
  if ($row_ref) {
    return @$row_ref;
  } else {
    return ();
  }

}

sub getrow_ref {

  my $self = shift;
  my $row_ref = $self->{data}->[$_[0]];
  if ($row_ref) {
    return $row_ref;
  } else {
    return [];
  }

}

sub getcol {

  my $self = shift;
  my @col = ();
  for (my $i = 0; $i < $self->rowcount(); $i++) {
    push(@col,$self->{data}->[$i]->[$_[0]]);
  }
  return @col;

}

sub getcol_ref {

  my $self = shift;
  my @col = $self->getcol($_[0]);
  return \@col;

}

sub sortrows {

  my $self = shift;
  my $sortfunction = shift;
  my @new_rows = sort $sortfunction @{$self->{data}};
  #$self->_set_data(\@new_rows);
  # since sorting can not affect uniqueness of rows and rowhashes, we just set:
  $self->{data} = \@new_rows;

}

sub tostring {

  my $self = shift;
  my @rows = @{$self->{data}};
  my $result = '';
  my $row_ref;
  for (my $i = 0; $i < scalar @rows; $i++) {
    $row_ref = $rows[$i];
    $result .= join($_[0],@$row_ref) . $_[1];
  }
  return substr($result,0,length($result) - length($_[1]));

}

1;
