package FakeTime;
use strict;

## no critic

use Time::HiRes qw(); #prevent warning from Time::Warp
use Time::Warp qw();
use DateTime::TimeZone qw();
use DateTime::Format::Strptime qw();
use DateTime::Format::ISO8601 qw();

use Logging qw(
    getlogger
    faketimedebug
    faketimeinfo);

use LogError qw(
    faketimeerror
    restwarn);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    set_time
    get_now
    current_unix
    infinite_future
    is_infinite_future
    datetime_to_string
    datetime_from_string
    );

#my $logger = getlogger(__PACKAGE__);

my $is_fake_time = 0;    
    
sub set_time {
	my ($o) = @_;
	if (defined $o) {
		_set_fake_time($o);
		my $now = _current_local();
		faketimeinfo("applying fake time offset '$o' - current time: " . datetime_to_string($now),getlogger(__PACKAGE__));
	} else {
		_set_fake_time();
		my $now = _current_local();
		faketimeinfo("resetting fake time - current time: " . datetime_to_string($now),getlogger(__PACKAGE__));
	}
}

sub _get_fake_clienttime_now {
	return datetime_to_string(_current_local());
}

sub get_now {
	return _current_local();
}    
    
sub current_unix {
	if ($is_fake_time) {
		return Time::Warp::time;
	} else {
		time;
	}
}

sub _current_local {
	if ($is_fake_time) {
		return DateTime->from_epoch(epoch => Time::Warp::time,
			time_zone => DateTime::TimeZone->new(name => 'local')
		);
	} else {
		return DateTime->now(
			time_zone => DateTime::TimeZone->new(name => 'local')
		);
	}
}

sub infinite_future {
	#... to '9999-12-31 23:59:59'
	return DateTime->new(year => 9999, month => 12, day => 31, hour => 23, minute => 59, second => 59,
		#applying the 'local' timezone takes too long -> "The current implementation of DateTime::TimeZone
		#will use a huge amount of memory calculating all the DST changes from now until the future date.
		#Use UTC or the floating time zone and you will be safe."
		time_zone => DateTime::TimeZone->new(name => 'UTC')
		#- with floating timezones, the long conversion takes place when comparing with a 'local' dt
		#- the error due to leap years/seconds is not relevant in comparisons
	);
}

sub is_infinite_future {
	my $dt = shift;
	return $dt->year >= 9999;
}

sub datetime_to_string {
	my ($dt) = @_;
	return unless defined ($dt);
	my $s = $dt->ymd('-') . ' ' . $dt->hms(':');
	$s .= '.'.$dt->millisecond if $dt->millisecond > 0.0;
	return $s;
}

sub datetime_from_string {
	my $s = shift;
	$s =~ s/^(\d{4}\-\d{2}\-\d{2})\s+(\d.+)$/$1T$2/;
	my $ts = DateTime::Format::ISO8601->parse_datetime($s);
	$ts->set_time_zone( DateTime::TimeZone->new(name => 'local') );
	return $ts;
}

sub _set_fake_time {
	my ($o) = @_;
	$is_fake_time = 1;
	if (defined $o) {
		if (ref $o eq 'DateTime') {
			$o = $o->epoch;
		} else {
			my %mult = (
				s => 1,
				m => 60,
				h => 60*60,
				d => 60*60*24,
				M => 60*60*24*30,
				y => 60*60*24*365,
			);

			if (!$o) {
				$o = time;
			} elsif ($o =~ m/^([+-]\d+)([smhdMy]?)$/) {
				$o = time + $1 * $mult{ $2 || "s" };
			} elsif ($o !~ m/\D/) {

			} else {
				faketimeerror("Invalid time offset: '$o'",getlogger(__PACKAGE__));
			}
		}
		Time::Warp::to($o);
	} else {
		Time::Warp::reset();
		$is_fake_time = 0;
	}
}

1;