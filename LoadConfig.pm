package LoadConfig;
use strict;

use Globals qw(
    $application_path
    update_mainconfig
    log_mainconfig
);

use Logging qw(
    getlogger
    mainconfigurationloaded
    configinfo
    init_log4perl
);

use LogError qw(
    fileerror
    yamlerror
    configurationwarn
    configurationerror
    parameterdefinedtwice
);

use YAML::Tiny;
use Utils qw(format_number);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    $loadedmainconfigfile
    load_config
);

our $loadedmainconfigfile;

my $tuplesplitpattern = join('|',(quotemeta(','),
                                  quotemeta(';'),
                                  quotemeta('/')
                                  )
                             );

my $logger = getlogger(__PACKAGE__);

sub load_config {

    my ($configfile,$process_code,$configtype) = @_;
    
    my $data;
    if (defined $configfile) {
        if (-e $configfile) {
            $data = _parse_config($file,$configtype);
        } else {
            $configfile = $application_path . $configfile;
            if (-e $configfile) {
                $data = _parse_config($configfile,$configtype);
            } else {
                fileerror('cannot find config file ' . $configfile,$logger);
            }
        }
    } else {
        configurationerror('no config file specified',$logger);
    }
    
    if ('CODE' eq ref $process_code) {
        my $result = @$process_code($data);
        configinfo('configuration file ' . $configfile . ' loaded',$logger);
        return $result;
    } else {
        if (update_mainconfig($data,$configfile,
                          \&split_tuple,
                          \&format_number,
                          \&configurationwarn,
                          \&configurationerror,
                          $logger)) {
            $loadedmainconfigfile = $configfile;
            mainconfigurationloaded($configfile,$logger);
            return 1;
        }
        log_mainconfig(\&configinfo,$logger);
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
    fileerror('parse simple config - cannot open file ' . $file . ': ' . $!,$logger);
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
        parameterdefinedtwice('parse simple config - parameter ' . $key . ' defined twice in line ' . $count . ' of configuration file ' . $file,$logger);
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
        yamlerror('parse yaml config - error reading file ' . $file . ': ' . $!,$logger);
        return $yaml;
    }
    
    return $yaml;
  
}

1;