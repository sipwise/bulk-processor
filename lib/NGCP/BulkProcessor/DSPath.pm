package NGCP::BulkProcessor::DSPath;
use strict;

#use 5.006001;
use warnings;
use Scalar::Util qw/reftype blessed/;
#use Carp;

#our $VERSION = '1.4.1';

# this is a reformatted variant of https://metacpan.org/pod/release/ZAPHAR/Data-Path-1.4.1/lib/Data/Path.pm
# for local control. the only functional difference so far is that path expressions use . instead of /

sub new {
	my ($class, $data, $callbacks) = @_;
	$callbacks //= {};
	my $self = {
        data => $data,
		# set call backs to default if not given
		callbacks => {
            key_does_not_exist => $callbacks->{key_does_not_exist} // sub {
				my ($data, $key, $index, $value, $rest) = @_;
				die "key $key does not exists\n";
            },
            index_does_not_exist => $callbacks->{index_does_not_exist} // sub {
				my ($data, $key, $index, $value, $rest) = @_;
				die "index $key\[$index\] does not exists\n";
			},
            retrieve_index_from_non_array => $callbacks->{retrieve_index_from_non_array} // sub {
				my ($data, $key, $index, $value, $rest) = @_;
				die "tried to retrieve an index $index from a no array value (in key $key)\n";
			},
            retrieve_key_from_non_hash => $callbacks->{retrieve_key_from_non_hash} // sub {
				my ($data, $key, $index, $value, $rest) = @_;
				die "tried to retrieve a key from a no hash value (in key $key)\n";
			},
            not_a_coderef_or_method => $callbacks->{not_a_coderef_or_method} // sub {
				my ($data, $key, $index, $value, $rest) = @_;
				die "tried to retrieve from a non-existent coderef or method: $key in $data\n";
			},
			filter => $callbacks->{filter} // sub {
				my ($item_path) = @_;
				die "no filter callback method provided\n";
			},
			transform => $callbacks->{transform} // sub {
				my ($item_path) = @_;
				die "no transform callback method provided\n";
			},
		},
	};
	return bless $self,$class;
}

sub get {
	my ($self,$rkey,$data) = @_;

	# set data to
	$data //= $self->{data};

	# get key till . or [
	my $key;
	$key = $1 if ( $rkey =~ s/^\.([^\.|\[]+)// );
    die 'malformed path expression' unless $key;
    die 'malformed array index request' if $rkey =~ /^\[([^\d]*)\]/;
	# check index for index
	my $index;
	$index = $1 if ( $rkey =~ s/^\[(\d+)\]// );
	# set rest
	my $rest  = $rkey;
	# get key from data
	my $value;
    if ($key =~ s/(\(\))$//) {
        $self->{callbacks}->{not_a_coderef_or_method}->($data, $key, $index, $value, $rest)
            unless exists $data->{$key} or (blessed $data and $data->can($key));

        $value = $data->{$key}->() if exists $data->{$key};
        $value = $data->$key() if (blessed $data and $data->can($key));
    } else {
	   $value = $data->{$key} if exists $data->{$key};
    }

    # croak if key does not exists and something after that is requested
	$self->{callbacks}->{key_does_not_exist}->($data, $key, $index, $value, $rest)
		if (not exists $data->{$key} and length($rest) > 0);

	# check index
	if (defined $index) {

		# croak if index does not exists and something after that is requested
		$self->{callbacks}->{index_does_not_exist}->($data, $key, $index, $value, $rest)
			if (not exists $value->[$index] and length($rest) > 0);

		if (reftype $value eq 'ARRAY') {
			$value = $value->[$index];
		} else {
			$self->{callbacks}->{retrieve_index_from_non_array}->($data, $key, $index, $value, $rest);
		}
	}

	# check if last element is reached
	if ($rest) {
		if (defined $value and (reftype $value eq 'HASH' or blessed $value)) {
			$value = $self->get($rest,$value);
		} else {
			$self->{callbacks}->{retrieve_key_from_non_hash}->($data, $key, $index, $value, $rest);
		}
	}

	return $value;
}

# filter graph (in-place)
sub filter {
	my ($self,$data,$path) = @_;

	# set data to
	$data //= $self->{data};
	
	$self->_filter($data,$path);
	
	return $self; #support chaining
}

sub _filter {
	my ($self,$data,$path) = @_;
	
	my $reftype = reftype $data;
	return unless $reftype;
	my $include = 0;
	if ("ARRAY" eq $reftype) {
		my ($i,$j) = (0, 0);
		foreach (@$data) {
			my $item_path = (length($path) ? $path : '') . '[' . $j . ']';
			if ($self->{callbacks}->{filter}->($item_path)
				or $self->_filter($data->[$i],$item_path)) {
				$include = 1;
				$i++;
			} else {
				splice(@$data, $i, 1);
			}
			$j++;
		}
	} elsif ("HASH" eq $reftype) {
		foreach my $key (keys %$data) {
			my $item_path = (length($path) ? $path . '.' : '') . $key;
			if ($self->{callbacks}->{filter}->($item_path)
				or $self->_filter($data->{$key},$item_path)) {
				$include = 1;
			} else {
				delete $data->{$key};
			}
		}
	}
	return $include;
	
}

# transform graph (in-place)
sub transform {
	my ($self,$data,$path) = @_;

	# set data to
	$data //= $self->{data};
	
	$self->_transform($data,$path);
	
	return $self; #support chaining
}

sub _transform {
	my ($self,$data,$path) = @_;
	
	my $reftype = reftype $data;
	return unless $reftype;
	if ("ARRAY" eq $reftype) {
		my $i = 0;
		foreach (@$data) {
			my $item_path = (length($path) ? $path : '') . '[' . $i . ']';
			$self->_transform($data->[$i],$item_path);
			$data->[$i] = $self->{callbacks}->{transform}->($data->[$i],$item_path);
			$i++;
		}
	} elsif ("HASH" eq $reftype) {
		foreach my $key (keys %$data) {
			my $item_path = (length($path) ? $path . '.' : '') . $key;
			$self->_transform($data->{$key},$item_path);
			$data->{$key} = $self->{callbacks}->{transform}->($data->{$key},$item_path);
		}
	}
	
}

1;
