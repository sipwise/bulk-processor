package NGCP::BulkProcessor::Calendar;
use strict;

## no critic

use DateTime qw();
use Time::HiRes qw(); #prevent warning from Time::Warp
use Time::Warp qw();
use DateTime::TimeZone qw();
use DateTime::Format::Strptime qw();
use DateTime::Format::ISO8601 qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
    faketimedebug
    faketimeinfo);

use NGCP::BulkProcessor::LogError qw(
    faketimeerror
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    set_fake_time
    get_fake_now
    get_fake_now_string
    fake_current_unix
    infinite_future
    is_infinite_future
    infinite_past
    is_infinite_past
    datetime_to_string
    datetime_from_string
    set_timezone
);

my $is_fake_time = 0;
my $timezone_cache = {};
my $UTC = DateTime::TimeZone->new(name => 'UTC');
my $LOCAL = DateTime::TimeZone->new(name => 'local');
my $FLOATING = DateTime::TimeZone::Floating->new();

sub set_fake_time {
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

sub get_fake_now_string {
	return datetime_to_string(_current_local());
}

sub get_fake_now {
	return _current_local();
}

sub fake_current_unix {
	if ($is_fake_time) {
		return Time::Warp::time;
	} else {
		time;
	}
}

sub _current_local {
	if ($is_fake_time) {
		return DateTime->from_epoch(epoch => Time::Warp::time, time_zone => $LOCAL);
	} else {
		return DateTime->now(time_zone => $LOCAL);
	}
}

sub infinite_future {
	#... to '9999-12-31 23:59:59'
	return DateTime->new(year => 9999, month => 12, day => 31, hour => 23, minute => 59, second => 59,
		#applying the 'local' timezone takes too long -> "The current implementation of DateTime::TimeZone
		#will use a huge amount of memory calculating all the DST changes from now until the future date.
		#Use UTC or the floating time zone and you will be safe."
		time_zone => $UTC
		#- with floating timezones, the long conversion takes place when comparing with a 'local' dt
		#- the error due to leap years/seconds is not relevant in comparisons
	);
}

sub is_infinite_future {
	my $dt = shift;
	return $dt->year >= 9999;
}

sub infinite_past {
    #mysql 5.5: The supported range is '1000-01-01 00:00:00' ...
    return DateTime->new(year => 1000, month => 1, day => 1, hour => 0, minute => 0, second => 0,
        time_zone => $UTC
    );
    #$dt->epoch calls should be okay if perl >= 5.12.0
}

sub is_infinite_past {
    my $dt = shift;
    return $dt->year <= 1000;
}

sub datetime_to_string {
	my ($dt) = @_;
	return unless defined ($dt);
	my $s = $dt->ymd('-') . ' ' . $dt->hms(':');
	$s .= '.'.$dt->millisecond if $dt->millisecond > 0.0;
	return $s;
}

sub datetime_from_string {
	my ($s,$tz) = @_;
	$s =~ s/^(\d{4}\-\d{2}\-\d{2})\s+(\d.+)$/$1T$2/;
	my $ts = DateTime::Format::ISO8601->parse_datetime($s);
    set_timezone($ts,$tz);
	return $ts;
}

sub set_timezone {
    my ($dt,$tz) = @_;
    return unless defined ($dt);
    if (defined $tz and length($tz) > 0) {
        my $timezone;
        if (exists $timezone_cache->{$tz}) {
            $timezone = $timezone_cache->{$tz};
        } else {
            $timezone = DateTime::TimeZone->new(name => $tz);
            $timezone_cache->{$tz} = $timezone;
        }
        $dt->set_time_zone( $timezone );
    } else { #floating otherwise.
        $dt->set_time_zone( $FLOATING );
    }
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
