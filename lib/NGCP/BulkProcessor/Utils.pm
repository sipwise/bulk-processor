package NGCP::BulkProcessor::Utils;
use strict;

## no critic

use threads;

#use POSIX qw(strtod);
use POSIX qw(strtod locale_h floor fmod);
setlocale(LC_NUMERIC, 'C');

use List::Util qw(max min);

use Data::UUID qw();
use UUID qw();

use Data::Validate::IP qw(is_ipv4 is_ipv6);

use Net::Address::IP::Local qw();
#use FindBin qw($Bin);
#use File::Spec::Functions qw(splitdir catdir);
use Net::Domain qw(hostname hostfqdn hostdomain);

use Cwd qw(abs_path);
#use File::Basename qw(fileparse);

use Time::Piece;
use Time::Seconds;
use Time::Local;
use Date::Manip qw(Date_Init ParseDate UnixDate);
#Date_Init('Language=English','DateFormat=non-US');
Date_Init('DateFormat=US');
#use Net::Address::IP::Local;

use Date::Calc qw(Normalize_DHMS Add_Delta_DHMS);

use Text::Wrap qw();
#use FindBin qw($Bin);
use Digest::MD5 qw(); #qw(md5 md5_hex md5_base64);
use File::Temp qw(tempfile tempdir);
use File::Path qw(remove_tree make_path);

#use Sys::Info;
#use Sys::Info::Constants qw( :device_cpu );

# after all, the only reliable way to get the true vCPU count:
use Sys::CpuAffinity; # qw(getNumCpus); not exported?
#disabling for now, no debian package yet.

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    float_equal
    round
    stringtobool
    booltostring
    check_bool
    tempfilename
    timestampdigits
    datestampdigits
    parse_datetime
    parse_date
    timestampfromdigits
    datestampfromdigits
    timestamptodigits
    datestamptodigits
    timestamptostring
    timestampaddsecs
    file_md5
    cat_file
    wrap_text
    create_guid
    create_uuid
    urlencode
    urldecode
    timestamp
    datestamp
    timestamp_fromepochsecs
    get_year
    get_year_month
    get_year_month_day
    to_duration_string
    secs_to_years
    zerofill
    trim
    chopstring
    get_ipaddress
    get_hostfqdn
    getscriptpath

    kbytes2gigs
    cleanupdir
    fixdirpath
    threadid
    format_number

    dec2bin
    bin2dec

    check_number
    min_timestamp
    max_timestamp
    add_months
    makepath
    changemod

    get_cpucount

    $chmod_umask

    prompt
    check_int
    check_ipnet
);

our $chmod_umask = 0777;

my $default_epsilon = 1e-3; #float comparison tolerance

sub float_equal {

    my ($a, $b, $epsilon) = @_;
    if ((!defined $epsilon) || ($epsilon <= 0.0)) {
        $epsilon = $default_epsilon;
    }
    return (abs($a - $b) < $epsilon);

}

sub round {

    my ($number) = @_;
    return int($number + .5 * ($number <=> 0));

}

sub stringtobool {

  my $inputstring = shift;
  if (lc($inputstring) eq 'y' or lc($inputstring) eq 'true' or $inputstring >= 1) {
    return 1;
  } else {
    return 0;
  }

}

sub booltostring {

  if (shift) {
    return 'true';
  } else {
    return 'false';
  }

}

sub check_bool {

  my $inputstring = shift;
  if (lc($inputstring) eq 'y' or lc($inputstring) eq 'true' or $inputstring >= 1) {
    return 1;
  } elsif (lc($inputstring) eq 'n' or lc($inputstring) eq 'false' or $inputstring == 0) {
    return 1;
  } else {
    return 0;
  }

}

sub timestampdigits {
  return localtime(shift)->strftime('%Y%m%d%H%M%S');
}

sub datestampdigits {
  return localtime(shift)->strftime('%Y%m%d');
}

sub timestamp {
  return localtime(shift)->strftime('%Y-%m-%d %H:%M:%S');
}

# Compat alias
sub timestamp_fromepochsecs {
  return timestamp(shift);
}

sub datestamp {
  return localtime(shift)->strftime('%Y-%m-%d');
}

sub get_year {
  return localtime(shift)->strftime('%Y');
}

sub get_year_month {
  my $t = localtime(shift);

  return ($t->strftime('%Y'), $t->strftime('%m'));
}

sub get_year_month_day {
  my $t = localtime(shift);

  return ($t->strftime('%Y'), $t->strftime('%m'), $t->strftime('%d'));
}

sub parse_datetime {

  my ($datetimestring,$non_us) = @_;
  if ($non_us) {
    Date_Init('DateFormat=non-US');
  } else {
    Date_Init('DateFormat=US');
  }
  my $datetime = ParseDate($datetimestring);
  if (!$datetime) {
    return undef;
  } else {
    my ($year,$mon,$mday,$hour,$min,$sec) = UnixDate($datetime,"%Y","%m","%d","%H","%M","%S");
    return sprintf "%4d-%02d-%02d %02d:%02d:%02d",$year,$mon,$mday,$hour,$min,$sec;
  }

}

sub parse_date {

  my ($datetimestring,$non_us) = @_;
  if ($non_us) {
    Date_Init('DateFormat=non-US');
  } else {
    Date_Init('DateFormat=US');
  }
  my $datetime = ParseDate($datetimestring);
  if (!$datetime) {
    return undef;
  } else {
    my ($year,$mon,$mday) = UnixDate($datetime,"%Y","%m","%d");
    return sprintf "%4d-%02d-%02d",$year,$mon,$mday;
  }

}

sub timestampfromdigits {

  my ($timestampdigits) = @_;
  if ($timestampdigits =~ /^[0-9]{14}$/g) {
    return substr($timestampdigits,0,4) . '-' .
         substr($timestampdigits,4,2) . '-' .
         substr($timestampdigits,6,2) . ' ' .
         substr($timestampdigits,8,2) . ':' .
         substr($timestampdigits,10,2) . ':' .
         substr($timestampdigits,12,2);
  } else {
    return $timestampdigits;
  }

}

sub datestampfromdigits {

  my ($datestampdigits) = @_;
  if ($datestampdigits =~ /^[0-9]{8}$/g) {
    return substr($datestampdigits,0,4) . '-' .
         substr($datestampdigits,4,2) . '-' .
         substr($datestampdigits,6,2);
  } else {
    return $datestampdigits;
  }

}

sub timestamptodigits {

  my ($datetimestring,$non_us) = @_;
  if ($non_us) {
    Date_Init('DateFormat=non-US');
  } else {
    Date_Init('DateFormat=US');
  }
  my $datetime = ParseDate($datetimestring);
  if (!$datetime) {
    return '0';
  } else {
    my ($year,$mon,$mday,$hour,$min,$sec) = UnixDate($datetime,"%Y","%m","%d","%H","%M","%S");
    return sprintf "%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$sec;
  }

}

sub datestamptodigits {

  my ($datestring,$non_us) = @_;
  if ($non_us) {
    Date_Init('DateFormat=non-US');
  } else {
    Date_Init('DateFormat=US');
  }
  my $datetime = ParseDate($datestring);
  if (!$datetime) {
    return '0';
  } else {
    my ($year,$mon,$mday) = UnixDate($datetime,"%Y","%m","%d");
    return sprintf "%4d%02d%02d",$year,$mon,$mday;
  }

}

sub timestamptostring {

    Date_Init('DateFormat=US');
    return UnixDate(@_);

}

sub timestampaddsecs {

  my ($datetimestring,$timespan,$non_us) = @_;

  if ($non_us) {
    Date_Init('DateFormat=non-US');
  } else {
    Date_Init('DateFormat=US');
  }

  my $datetime = ParseDate($datetimestring);

  if (!$datetime) {

    return $datetimestring;

  } else {

    my ($fromyear,$frommonth,$fromday,$fromhour,$fromminute,$fromsecond) = UnixDate($datetime,"%Y","%m","%d","%H","%M","%S");

    my ($Dd,$Dh,$Dm,$Ds) = Date::Calc::Normalize_DHMS(0,0,0,$timespan);
    my ($toyear,$tomonth,$to_day,$tohour,$tominute,$tosecond) = Date::Calc::Add_Delta_DHMS($fromyear,$frommonth,$fromday,$fromhour,$fromminute,$fromsecond,
                                                                                           $Dd,$Dh,$Dm,$Ds);

    return sprintf "%4d-%02d-%02d %02d:%02d:%02d",$toyear,$tomonth,$to_day,$tohour,$tominute,$tosecond;

  }

}

sub tempfilename {

   my ($template,$path,$suffix) = @_;
   my ($tmpfh,$tmpfilename) = tempfile($template,DIR => $path,SUFFIX => $suffix);
   close $tmpfh;
   return $tmpfilename;

}

sub file_md5 {

    my ($filepath,$fileerrorcode,$logger) = @_;

    local *MD5FILE;

    if (not open (MD5FILE, '<' . $filepath)) {
      if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
        &$fileerrorcode('md5sum - cannot open file ' . $filepath . ': ' . $!,$logger);
      }
      return '';
    }
    binmode MD5FILE;
    my $md5digest = Digest::MD5->new->addfile(*MD5FILE)->hexdigest;
    close MD5FILE;
    return $md5digest;

}

sub cat_file {

    my ($filepath,$fileerrorcode,$logger) = @_;

    if (not open (CATFILE, '<' . $filepath)) {
      if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
        &$fileerrorcode('cat - cannot open file ' . $filepath . ': ' . $!,$logger);
      }
      return '';
    }
    my @linebuffer = <CATFILE>;
    close CATFILE;
    return join("\n",@linebuffer);

}

sub wrap_text {

    my ($inputstring, $columns) = @_;
    $Text::Wrap::columns = $columns;
    return Text::Wrap::wrap("","",$inputstring);

}

sub create_guid {

  my $ug = new Data::UUID;
  my $uuid = $ug->create();
  return $ug->to_string( $uuid );

}

sub create_uuid {
    my ($bin, $str);
    UUID::generate($bin);
    UUID::unparse($bin, $str);
    return $str;
}

sub urlencode {
  my ($urltoencode) = @_;
  $urltoencode =~ s/([^a-zA-Z0-9\/_\-.])/uc sprintf("%%%02x",ord($1))/eg;
  return $urltoencode;
}

sub urldecode {
  my ($urltodecode) = @_;
  $urltodecode =~ s/%([\dA-Fa-f][\dA-Fa-f])/pack ("C", hex ($1))/eg;
  return $urltodecode;
}

sub zerofill {
  my ($integer,$digits) = @_;

  return sprintf '%0*d', $digits, $integer;
}

sub trim {
  my ($inputstring) = @_;

  $inputstring =~ s/[\n\r\t]/ /g;
  $inputstring =~ s/^ +//;
  $inputstring =~ s/ +$//;

  return $inputstring;
}

sub chopstring {

  my ($inputstring,$trimlength,$ending) = @_;

  my $result = $inputstring;

  if (defined $inputstring) {

    $result =~ s/[\n\r\t]/ /g;

    if (!defined $trimlength) {
      $trimlength = 30;
    }
    if (!defined $ending) {
      $ending = '...'
    }

    if (length($result) > $trimlength) {
      return substr($result,0,$trimlength-length($ending)) . $ending;
    }
  }

  return $result;

}

sub get_ipaddress {

  # Get the local system's IP address that is "en route" to "the internet":
  return Net::Address::IP::Local->public;

}

sub get_hostfqdn {

    return hostfqdn();

}

sub getscriptpath {

  return abs_path($0);

}

my @unit_suffix = (
    'kBytes',
    'MBytes',
    'GBytes',
);

sub kbytes2gigs {
    my ($number, $base, $round_integer) = @_;

    $base = 1024 if $base <= 0;

    my $unit = 0;
    while ($unit < @unit_suffix && $number >= $base) {
        # We only want two decimals of precision.
        $number = int(($number / $base) * 100) / 100;
        $unit++;
    }

    $number = int $number if $round_integer;

    return "$number $unit_suffix[$unit]";
}

sub cleanupdir {

    my ($dirpath,$keeproot,$filewarncode,$logger) = @_;
    if (-d $dirpath) {
        remove_tree($dirpath, {
                'keep_root' => $keeproot,
                'verbose' => 1,
                'error' => \my $err });
        if (@$err) {
            if (defined $filewarncode and ref $filewarncode eq 'CODE') {
                for my $diag (@$err) {
                    my ($file, $message) = %$diag;
                    if ($file eq '') {
                        &$filewarncode("cleanup: $message",$logger);
                    } else {
                        &$filewarncode("problem unlinking $file: $message",$logger);
                    }
                }
            }
        }
        #else {
        #    if (!$keeproot and defined $scriptinfocode and ref $scriptinfocode eq 'CODE') {
        #        &$scriptinfocode($dirpath . ' removed',$logger);
        #    }
        #}
        #if ($restoredir) {
        #      makedir($dirpath);
        #}
    }

}

sub fixdirpath {
    my ($dirpath) = @_;
    $dirpath .= '/' if $dirpath !~ m!/$!;
    return $dirpath;
}

sub makepath {
    my ($dirpath,$fileerrorcode,$logger) = @_;
    #print $chmod_umask ."\n";
    #changemod($dirpath);
    make_path($dirpath,{
        'chmod' => $chmod_umask,
        'verbose' => 1,
        'error' => \my $err });
    if (@$err) {
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
            for my $diag (@$err) {
                my ($file, $message) = %$diag;
                if ($file eq '') {
                    &$fileerrorcode("creating path: $message",$logger);
                } else {
                    &$fileerrorcode("problem creating $file: $message",$logger);
                }
            }
        }
        return 0;
    }
    return 1;
}

#sub makedir {
#    my ($dirpath,$fileerrorcode,$logger) = @_;
#    eval {
#        mkdir $dirpath;
#        chmod oct($chmod_umask),$dirpath;
#    };
#    if ($@) {
#        if (not -d $f_dir) {
#        fileerror('cannot opendir ' . $f_dir . ': ' . $!,getlogger(__PACKAGE__));
#        return;
#    }
#}

sub changemod {
    my ($filepath) = @_;
    chmod $chmod_umask,$filepath;
}

sub threadid {

    return threads->tid();
    #return threads->_handle();

}

sub format_number {
  my ($value,$decimals) = @_;
  my $output = $value;
  #if (index($output,',') > -1) {
  #  $output =~ s/,/\./g;
  #}
  if (defined $decimals and $decimals >= 0) {
    $output = round(($output * (10 ** ($decimals + 1))) / 10) / (10 ** $decimals);
    $output = sprintf("%." . $decimals . "f",$output);
    if (index($output,',') > -1) {
      $output =~ s/,/\./g;
    }
  } else {
    $output = sprintf("%f",$output);
    #if (index($output,',') > -1) {
    #  $output =~ s/,/\./g;
    #}
    if (index($output,'.') > -1) {
      $output =~ s/0+$//g;
      $output =~ s/\.$//g;
    }
  }
  return $output;
}

sub dec2bin {
    my $str = unpack("B32", pack("N", shift));
    $str =~ s/^0+(?=\d)//;   # leading zeros otherwise
    return $str;
}

sub bin2dec {
    return unpack("N", pack("B32", substr("0" x 32 . shift, -32)));
}

sub getnum {

    my $str = shift;
    $str =~ s/^\s+//;
    $str =~ s/\s+$//;
    $! = 0;
    my($num, $unparsed) = strtod($str);
    if (($str eq '') || ($unparsed != 0) || $!) {
        return;
    } else {
        return $num;
    }
}

sub check_number {

    my $potential_number = shift;
    if (defined getnum($potential_number)) {
        return 1;
    } else {
        return 0;
    }

}

sub min_timestamp {

    my (@timestamps) = @_;

    return min(@timestamps);

}

sub max_timestamp {

    my (@timestamps) = @_;

    return max(@timestamps);

}

sub add_months {
    my ($month, $year, $ads) = @_;

    if ($month > 0 and $month <= 12) {
        my $time = timelocal(0, 0, 0, 1, $month - 1, $year);
        my $t = Time::Piece->new($time)->add_months($ads);

        return ($t->mon, $t->year);
    } else {
        return (undef, undef);
    }
}

sub secs_to_years {

  my $time_in_secs = shift;

  return Time::Seconds->new($time_in_secs)->pretty;
}

sub to_duration_string {
    my ($duration_secs,$most_significant,$least_significant,$least_significant_decimals,$loc_code) = @_;
    $most_significant //= 'years';
    $least_significant //= 'seconds';
    #$loc_code //= sub { return shift; };
    my $abs = abs($duration_secs);
    my ($years,$months,$days,$hours,$minutes,$seconds);
    my $result = '';
    if ('seconds' ne $least_significant) {
        $abs = $abs / 60.0; #minutes
        if ('minutes' ne $least_significant) {
            $abs = $abs / 60.0; #hours
            if ('hours' ne $least_significant) {
                $abs = $abs / 24.0; #days
                if ('days' ne $least_significant) {
                    $abs = $abs / 30.0; #months
                    if ('months' ne $least_significant) {
                        $abs = $abs / 12.0; #years
                        if ('years' ne $least_significant) {
                            die("unknown least significant duration unit-of-time: '$least_significant'");
                        } else {
                            $seconds = 0.0;
                            $minutes = 0.0;
                            $hours = 0.0;
                            $days = 0.0;
                            $months = 0.0;
                            if ('years' eq $most_significant) {
                                $years = $abs;
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    } else {
                        $seconds = 0.0;
                        $minutes = 0.0;
                        $hours = 0.0;
                        $days = 0.0;
                        $years = 0.0;
                        if ('months' eq $most_significant) {
                            $months = $abs;
                        } else {
                            $months = ($abs >= 12.0) ? fmod($abs,12.0) : $abs;
                            $abs = $abs / 12.0;
                            if ('years' eq $most_significant) {
                                $years = floor($abs);
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    }
                } else {
                    $seconds = 0.0;
                    $minutes = 0.0;
                    $hours = 0.0;
                    $months = 0.0;
                    $years = 0.0;
                    if ('days' eq $most_significant) {
                        $days = $abs;
                    } else {
                        $days = ($abs >= 30.0) ? fmod($abs,30.0) : $abs;
                        $abs = $abs / 30.0;
                        if ('months' eq $most_significant) {
                            $months = floor($abs);
                        } else {
                            $months = ($abs >= 12.0) ? fmod($abs,12.0) : $abs;
                            $abs = $abs / 12.0;
                            if ('years' eq $most_significant) {
                                $years = floor($abs);
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    }
                }
            } else {
                $seconds = 0.0;
                $minutes = 0.0;
                $days = 0.0;
                $months = 0.0;
                $years = 0.0;
                if ('hours' eq $most_significant) {
                    $hours = $abs;
                } else {
                    $hours = ($abs >= 24.0) ? fmod($abs,24.0) : $abs;
                    $abs = $abs / 24.0;
                    if ('days' eq $most_significant) {
                        $days = floor($abs);
                    } else {
                        $days = ($abs >= 30.0) ? fmod($abs,30) : $abs;
                        $abs = $abs / 30.0;
                        if ('months' eq $most_significant) {
                            $months = floor($abs);
                        } else {
                            $months = ($abs >= 12.0) ? fmod($abs,12.0) : $abs;
                            $abs = $abs / 12.0;
                            if ('years' eq $most_significant) {
                                $years = floor($abs);
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    }
                }
            }
        } else {
            $seconds = 0.0;
            $hours = 0.0;
            $days = 0.0;
            $months = 0.0;
            $years = 0.0;
            if ('minutes' eq $most_significant) {
                $minutes = $abs;
            } else {
                $minutes = ($abs >= 60.0) ? fmod($abs,60.0) : $abs;
                $abs = $abs / 60.0;
                if ('hours' eq $most_significant) {
                    $hours = floor($abs);
                } else {
                    $hours = ($abs >= 24.0) ? fmod($abs,24.0) : $abs;
                    $abs = $abs / 24.0;
                    if ('days' eq $most_significant) {
                        $days = floor($abs);
                    } else {
                        $days = ($abs >= 30.0) ? fmod($abs,30.0) : $abs;
                        $abs = $abs / 30.0;
                        if ('months' eq $most_significant) {
                            $months = floor($abs);
                        } else {
                            $months = ($abs >= 12.0) ? fmod($abs,12.0) : $abs;
                            $abs = $abs / 12.0;
                            if ('years' eq $most_significant) {
                                $years = floor($abs);
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    }
                }
            }
        }
    } else {
        $minutes = 0.0;
        $hours = 0.0;
        $days = 0.0;
        $months = 0.0;
        $years = 0.0;
        if ('seconds' eq $most_significant) {
            $seconds = $abs;
        } else {
            $seconds = ($abs >= 60.0) ? fmod($abs,60.0) : $abs;
            $abs = $abs / 60.0;
            if ('minutes' eq $most_significant) {
                $minutes = floor($abs);
            } else {
                $minutes = ($abs >= 60.0) ? fmod($abs,60.0) : $abs;
                $abs = $abs / 60.0;
                if ('hours' eq $most_significant) {
                    $hours = floor($abs);
                } else {
                    $hours = ($abs >= 24.0) ? fmod($abs,24.0) : $abs;
                    $abs = $abs / 24.0;
                    if ('days' eq $most_significant) {
                        $days = floor($abs);
                    } else {
                        $days = ($abs >= 30.0) ? fmod($abs,30.0) : $abs;
                        $abs = $abs / 30.0;
                        if ('minutes' eq $most_significant) {
                            $months = floor($abs);
                        } else {
                            $months = ($abs >= 12.0) ? fmod($abs,12.0) : $abs;
                            $abs = $abs / 12.0;
                            if ('years' eq $most_significant) {
                                $years = floor($abs);
                            } else {
                                die("most significant duration unit-of-time '$most_significant' lower than least significant duration unit-of-time '$least_significant'");
                            }
                        }
                    }
                }
            }
        }
    }
    if ($years > 0.0) {
        if ($months > 0.0 || $days > 0.0 || $hours > 0.0 || $minutes > 0.0 || $seconds > 0.0) {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$years, 0, 'years');
        } else {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$years, $least_significant_decimals, 'years');
        }
    }
    if ($months > 0.0) {
        if ($years > 0.0) {
            $result .= ', ';
        }
        if ($days > 0.0 || $hours > 0.0 || $minutes > 0.0 || $seconds > 0.0) {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$months, 0, 'months');
        } else {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$months, $least_significant_decimals, 'months');
        }
    }
    if ($days > 0.0) {
        if ($years > 0.0 || $months > 0.0) {
            $result .= ', ';
        }
        if ($hours > 0.0 || $minutes > 0.0 || $seconds > 0.0) {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$days, 0, 'days');
        } else {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$days, $least_significant_decimals, 'days');
        }
    }
    if ($hours > 0.0) {
        if ($years > 0.0 || $months > 0.0 || $days > 0.0) {
            $result .= ', ';
        }
        if ($minutes > 0.0 || $seconds > 0.0) {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$hours, 0, 'hours');
        } else {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$hours, $least_significant_decimals, 'hours');
        }
    }
    if ($minutes > 0.0) {
        if ($years > 0.0 || $months > 0.0 || $days > 0.0 || $hours > 0.0) {
            $result .= ', ';
        }
        if ($seconds > 0.0) {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$minutes, 0, 'minutes');
        } else {
            $result .= _duration_unit_of_time_value_to_string($loc_code,$minutes, $least_significant_decimals, 'minutes');
        }
    }
    if ($seconds > 0.0) {
        if ($years > 0.0 || $months > 0.0 || $days > 0.0 || $hours > 0.0 || $minutes > 0.0) {
            $result .= ', ';
        }
        $result .= _duration_unit_of_time_value_to_string($loc_code,$seconds, $least_significant_decimals, 'seconds');
    }
    if (length($result) == 0) {
        $result .= _duration_unit_of_time_value_to_string($loc_code,0.0, $least_significant_decimals, $least_significant);
    }
    return ($result,$years,$months,$days,$hours,$minutes,$seconds);
}

sub _duration_unit_of_time_value_to_string {
    my ($loc_code,$value, $decimals, $unit_of_time) = @_;
    my $result = '';
    my $unit_label_plural = '';
    my $unit_label_singular = '';
    if (defined $loc_code) {
        if ('seconds' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('seconds');
            $unit_label_singular = ' ' . &$loc_code("second");
        } elsif ('minutes' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('minutes');
            $unit_label_singular = ' ' . &$loc_code("minute");
        } elsif ('hours' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('hours');
            $unit_label_singular = ' ' . &$loc_code("hour");
        } elsif ('days' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('days');
            $unit_label_singular = ' ' . &$loc_code("day");
        } elsif ('months' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('months');
            $unit_label_singular = ' ' . &$loc_code("month");
        } elsif ('years' eq $unit_of_time) {
            $unit_label_plural = ' ' . &$loc_code('years');
            $unit_label_singular = ' ' . &$loc_code("year");
        }
    }
    if ($decimals < 1) {
        if (int($value) == 1) {
            $result .= '1';
            $result .= $unit_label_singular;
        } else {
            $result .= int($value);
            $result .= $unit_label_plural;
        }
    } else {
        $result .= sprintf('%.' . $decimals . 'f', $value);
        $result .= $unit_label_plural;
    }
    return $result;
}

sub get_cpucount {
    my $cpucount = Sys::CpuAffinity::getNumCpus() + 0;
    return ($cpucount > 0) ? $cpucount : 1;
    #my $info = Sys::Info->new();
    #my $cpu  = $info->device('CPU'); # => %options );
    #print "cpuidentify:" . scalar($cpu->identify()) . "\n";
    #print "cpuidentify:" . scalar($cpu->identify()) . "\n";
    #my $cpucount = $cpu->count() + 0;
    #print "ht:" . $cpu->ht() . "\n";
    #if ($cpu->ht()) {
    #    $cpucount *= 2;
    #}

   #printf "CPU: %s\n", scalar($cpu->identify)  || 'N/A';
   #printf "CPU speed is %s MHz\n", $cpu->speed || 'N/A';
   #printf "There are %d CPUs\n"  , $cpu->count || 1;
   #printf "CPU load: %s\n"       , $cpu->load  || 0;
}

sub prompt {
  my ($query) = @_; # take a prompt string as argument
  local $| = 1; # activate autoflush to immediately show the prompt
  print $query;
  chomp(my $answer = <STDIN>);
  return $answer;
}

sub check_ipnet {
    my ($ipnet) = @_;
    my ($ip, $net) = split(/\//,$ipnet);
    if (is_ipv4($ip)) {
        if (defined $net) {
            return check_int($net) && $net >= 0 && $net <= 32;
        } else {
            return 1;
        }
    } elsif (is_ipv6($ip)) {
        if (defined $net) {
            return check_int($net) && $net >= 0 && $net <= 128;
        } else {
            return 1;
        }
    }
    return 0;
}

sub check_int {
    my $val = shift;
    if($val =~ /^[+-]?[0-9]+$/) {
        return 1;
    }
    return 0;
}

1;
