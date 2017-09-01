package NGCP::BulkProcessor::Utils;
use strict;

## no critic

use threads;

#use POSIX qw(strtod);
use POSIX qw(strtod locale_h);
setlocale(LC_NUMERIC, 'C');

use Data::UUID qw();
use UUID qw();

use Data::Validate::IP qw(is_ipv4 is_ipv6);

use Net::Address::IP::Local qw();
#use FindBin qw($Bin);
#use File::Spec::Functions qw(splitdir catdir);
use Net::Domain qw(hostname hostfqdn hostdomain);

use Cwd qw(abs_path);
#use File::Basename qw(fileparse);

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
#use Sys::CpuAffinity; # qw(getNumCpus); not exported?
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

    my ($number) = shift;
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

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return sprintf "%4d%02d%02d%02d%02d%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec;

}

sub datestampdigits {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return sprintf "%4d%02d%02d",$year+1900,$mon+1,$mday;

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

sub timestamp {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec;

}

sub timestamp_fromepochsecs {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(shift);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d",$year+1900,$mon+1,$mday,$hour,$min,$sec;

}

sub datestamp {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return sprintf "%4d-%02d-%02d",$year+1900,$mon+1,$mday;

}

sub get_year {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return (sprintf "%4d",$year+1900);

}

sub get_year_month {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return ((sprintf "%4d",$year+1900),(sprintf "%02d",$mon+1));

}

sub get_year_month_day {

  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
  return ((sprintf "%4d",$year+1900),(sprintf "%02d",$mon+1),(sprintf "%02d",$mday));

}

sub zerofill {
  my ($integer,$digits) = @_;
  my $numberofzeroes = $digits - length($integer);
  my $resultstring = $integer;
  if ($digits > 0) {
    for (my $i = 0; $i < $numberofzeroes; $i += 1) {
      $resultstring = "0" . $resultstring;
    }
  }
  return $resultstring;
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

sub kbytes2gigs {
   my ($TotalkBytes,$kbytebase,$round) = @_;

   if ($kbytebase <= 0) {
     $kbytebase = 1024;
   }

   my $TotalkByteskBytes = $TotalkBytes;
   my $TotalkBytesMBytes = $TotalkBytes;
   my $TotalkBytesGBytes = $TotalkBytes;

   my $rounded = 0;
   $TotalkByteskBytes = $TotalkBytes;
   $TotalkBytesMBytes = 0;
   $TotalkBytesGBytes = 0;

   if ($TotalkByteskBytes >= $kbytebase) {
     $TotalkBytesMBytes = int($TotalkByteskBytes / $kbytebase);
     $rounded = int(($TotalkByteskBytes * 100) / $kbytebase) / 100;
     if ($round) { # == 1) {
       $rounded = int($rounded);
     }
     $rounded .= " MBytes";
     $TotalkByteskBytes = $TotalkBytes - $TotalkBytesGBytes * $kbytebase * $kbytebase - $TotalkBytesMBytes * $kbytebase;
     if ($TotalkBytesMBytes >= $kbytebase) {
       $TotalkBytesGBytes = int($TotalkBytesMBytes / $kbytebase);
       $rounded = int(($TotalkBytesMBytes * 100) / $kbytebase) / 100;
       if ($round) { # == 1) {
         $rounded = int($rounded);
       }
       $rounded .= " GBytes";
       $TotalkBytesMBytes = int(($TotalkBytes - $TotalkBytesGBytes * $kbytebase * $kbytebase) / $kbytebase);
       $TotalkByteskBytes = $TotalkBytes - $TotalkBytesGBytes * $kbytebase * $kbytebase - $TotalkBytesMBytes * $kbytebase;
     }
   }

   if ($TotalkBytesGBytes == 0 && $TotalkBytesMBytes == 0) {
     $TotalkBytes .= " kBytes";
   } elsif ($TotalkBytesGBytes == 0) {
     $TotalkBytes = $rounded; # . " (" . $TotalkBytesMBytes . " MBytes " . $TotalkByteskBytes . " kBytes)";
     if ($round) { # == 1) {
       $TotalkBytes = $rounded;
     }
   } else {
     $TotalkBytes = $rounded; # . " (" . $TotalkBytesGBytes . " GBytes " . $TotalkBytesMBytes . " MBytes " . $TotalkByteskBytes . " kBytes)";
     if ($round) { # == 1) {
       $TotalkBytes = $rounded;
     }
   }
   return $TotalkBytes;
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

    my $min_ts = $timestamps[0];
    foreach my $ts (@timestamps) {
        if (($ts cmp $min_ts) < 0) {
            $min_ts = $ts;
        }
    }

    return $min_ts;

}

sub max_timestamp {

    my (@timestamps) = @_;

    my $min_ts = $timestamps[0];
    foreach my $ts (@timestamps) {
        if (($ts cmp $min_ts) > 0) {
            $min_ts = $ts;
        }
    }

    return $min_ts;

}

sub add_months {

  my ($month, $year, $ads) = @_;

  if ($month > 0  and $month <= 12) {

    my $sign = ($ads > 0) ? 1 : -1;
    my $rmonths = $month + $sign * (abs($ads) % 12);
    my $ryears = $year + int( $ads / 12 );

    if ($rmonths < 1) {
      $rmonths += 12;
      $ryears -= 1;
    } elsif ($rmonths > 12) {
      $rmonths -= 12;
      $ryears += 1;
    }

    return ($rmonths,$ryears);

  } else {

    return (undef,undef);

  }

}

sub secs_to_years {

  my $time_in_secs = shift;

  my $negative = 0;
  if ($time_in_secs < 0) {
    $time_in_secs *= -1;
    $negative = 1;
  }

  my $years = 0;
  my $months = 0;
  my $days = 0;
  my $hours = 0;
  my $mins = 0;
  my $secs = $time_in_secs;

  if ($secs >= 60) {
    $mins = int($secs / 60);
    $secs = ($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60-$mins*60);
    if ($mins >= 60) {
      $hours = int($mins / 60);
      $mins = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60) / (60));
      $secs = ($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60-$mins*60);
      if ($hours >= 24) {
        $days = int($hours / 24);
        $hours = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24) / (60*60));
        $mins = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60) / (60));
        $secs = ($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60-$mins*60);
        if ($days >= 30) {
          $months = int($days / 30);
          $days = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30) / (60*60*24));
          $hours = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24) / (60*60));
          $mins = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60) / (60));
          $secs = ($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60-$mins*60);
          if ($months >= 12) {
            $years = int($months / 12);
            $months = int(($time_in_secs-$years*60*60*24*30*12) / (60*60*24*30));
            $days = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30) / (60*60*24));
            $hours = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24) / (60*60));
            $mins = int(($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60) / (60));
            $secs = ($time_in_secs-$years*60*60*24*30*12-$months*60*60*24*30-$days*60*60*24-$hours*60*60-$mins*60);
          }
        }
      }
    }
  }

  $secs = zerofill(int($secs),2);
  $mins = zerofill($mins,2);
  $hours = zerofill($hours,2);

  if ($years == 0 && $months == 0 && $days == 0) {
    $time_in_secs = $hours . ':' . $mins . ':' . $secs;
  } elsif($years == 0 && $months == 0) {
    $time_in_secs = $days . ' day(s) - ' . $hours . ':' . $mins . ':' . $secs;
  } elsif($years == 0) {
    $time_in_secs = $months . ' month(s)/' . $days . ' day(s) - ' . $hours . ':' . $mins . ':' . $secs;
  } else {
    $time_in_secs = $years . ' year(s)/' . $months . ' month(s)/' . $days . ' day(s) - ' . $hours . ':' . $mins . ':' . $secs;
  }

  if ($negative == 1) {
    return '- ' . $time_in_secs;
  } else {
    return $time_in_secs;
  }
}

sub get_cpucount {
    my $cpucount = 0; #Sys::CpuAffinity::getNumCpus() + 0;
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
