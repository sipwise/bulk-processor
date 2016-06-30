package LoadConfig;
use strict;

## no critic

use Globals qw(
    $application_path
    $executable_path
    update_mainconfig
    log_mainconfig
);

use Logging qw(
    getlogger
    configurationinfo
);

use LogError qw(
    fileerror
    configurationwarn
    configurationerror
);

use YAML::Tiny;
use Utils qw(format_number);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    $loadedmainconfigfile
    load_config
);

our $loadedmainconfigfile = undef;

my $tuplesplitpattern = join('|',(quotemeta(','),
                                  quotemeta(';'),
                                  quotemeta('/')
                                  )
                             );

#my $logger = getlogger(__PACKAGE__);

sub load_config {

    my ($configfile,$process_code,$configtype) = @_;
    
    my $data;
    if (defined $configfile) {
        if (-e $configfile) {
            $data = _parse_config($configfile,$configtype);
        } else {
            my $relative_configfile = $executable_path . $configfile;
            if (-e $relative_configfile) {
                $configfile = $relative_configfile;
                $data = _parse_config($configfile,$configtype);
            } else {
                configurationwarn($configfile,'no project config file ' . $relative_configfile,getlogger(__PACKAGE__));
                $relative_configfile = $application_path . $configfile;
                if (-e $relative_configfile) {
                    $configfile = $relative_configfile;
                    $data = _parse_config($configfile,$configtype);
                } else {
                    configurationerror($configfile,'no global config file ' . $relative_configfile,getlogger(__PACKAGE__));
                    return 0;
                }
            }
        }
    } else {
        fileerror('no config file specified',getlogger(__PACKAGE__));
        return 0;
    }
    
    if ('CODE' eq ref $process_code) {
        my $result = &$process_code($data);
        configurationinfo('configuration file ' . $configfile . ' loaded',getlogger(__PACKAGE__));
        return $result;
    } else {
        if (update_mainconfig($data,$configfile,
                          \&split_tuple,
                          \&format_number,
                          \&configurationwarn,
                          \&configurationerror,
                          \&fileerror,
                          getlogger(__PACKAGE__))) {
            $loadedmainconfigfile = $configfile;
            #configurationinfo('master configuration file ' . $configfile . ' loaded',getlogger(__PACKAGE__));
            log_mainconfig(\&configurationinfo,getlogger(__PACKAGE__));
            return 1;
        }
        log_mainconfig(\&configurationinfo,getlogger(__PACKAGE__));
        return 0;
    }

}

sub _parse_config {
    my ($file,$configtype) = @_;
    my $data;
    if (defined $configtype) {
        if ($configtype == 1) {
            $data = _parse_yaml_config($file);
        } else {
            $data = _parse_simple_config($file);
        }
    } else {
        $data = _parse_simple_config($file);
    }
    return $data;
}

sub split_tuple {

    my $token = shift;
    return split(/$tuplesplitpattern/,$token);

}

#sub parse_float {
#
#  my ($value) = @_;
#  my $output = $value;
#  if (index($output,",") > -1) {
#    $output =~ s/,/\./g;
#  }
#  $output = sprintf("%f",$output);
#  #$output =~ s/0+$//g;
#  #$output =~ s/\.$//g;
#  #if ($output =~ /\..+/) {
#  #  $output =~ s/0+$//g;
#  #  $output =~ s/\.$//g;
#  #}
#  if (index($output,".") > -1) {
#    $output =~ s/0+$//g;
#    $output =~ s/\.$//g;
#  }
#  return $output;
#
#}

sub _parse_simple_config {

  my $file = shift;

  my $config = {};
  local *CF;

  if (not open (CF, '<' . $file)) {
    fileerror('parse simple config - cannot open file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
    return $config;
  }

  read(CF, my $data, -s $file);
  close(CF);

  my @lines  = split(/\015\012|\012|\015/,$data);
  my $count  = 0;

  foreach my $line(@lines) {
    $count++;

    next if($line =~ /^\s*#/);
    next if($line !~ /^\s*\S+\s*=.*$/);

    #my $cindex = index($line,'#');
    #if ($cindex >= 0) {
    #    $line = substr($line,0,$cindex);
    #}

    my ($key,$value) = split(/=/,$line,2);

    # Remove whitespaces at the beginning and at the end

    $key   =~ s/^\s+//g;
    $key   =~ s/\s+$//g;
    $value =~ s/^\s+//g;
    $value =~ s/\s+$//g;

    if (exists $config->{$key}) {
        configurationwarn($file,'parse simple config - parameter ' . $key . ' defined twice in line ' . $count,getlogger(__PACKAGE__));
    }

    $config->{$key} = $value;
    #print $key . "\n";
  }

  return $config;

}

sub _parse_yaml_config {

    my $file = shift;
  
    my $yaml = undef;
    eval {
        $yaml = YAML::Tiny->read($file);
    };
    if ($@) {
        configurationerror($file,'parse yaml config - error reading file: ' . $!,getlogger(__PACKAGE__));
        return $yaml;
    }
    
    return $yaml;
  
}

1;