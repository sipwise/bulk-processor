package NGCP::BulkProcessor::LoadConfig;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $system_name
    $system_version
    $system_instance_label
    $local_fqdn
    $application_path
    $working_path
    $executable_path
    $cpucount
    $enablemultithreading
    update_mainconfig
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    configurationinfo
);

use NGCP::BulkProcessor::LogError qw(
    fileerror
    configurationwarn
    configurationerror
);

use YAML::Tiny;
use NGCP::BulkProcessor::Utils qw(format_number);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    load_config
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
);

my $tuplesplitpattern = join('|',(quotemeta(','),
                                  quotemeta(';'),
                                  quotemeta('/')
                                  )
                             );

our $SIMPLE_CONFIG_TYPE = 1;
our $YAML_CONFIG_TYPE = 2;
#my $logger = getlogger(__PACKAGE__);

sub load_config {

    my ($configfile,$process_code,$configtype) = @_;

    my $is_settings = 'CODE' eq ref $process_code;
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
                configurationwarn($configfile,'no project ' . ($is_settings ? 'settings' : 'config') . ' file ' . $relative_configfile,getlogger(__PACKAGE__));
                $relative_configfile = $application_path . $configfile;
                if (-e $relative_configfile) {
                    $configfile = $relative_configfile;
                    $data = _parse_config($configfile,$configtype);
                } else {
                    configurationerror($configfile,'no global ' . ($is_settings ? 'settings' : 'config') . ' file ' . $relative_configfile,getlogger(__PACKAGE__));
                    return 0;
                }
            }
        }
    } else {
        fileerror('no ' . ($is_settings ? 'settings' : 'config') . ' file specified',getlogger(__PACKAGE__));
        return 0;
    }

    if ($is_settings) {
        my $result = &$process_code($data,$configfile,
                          \&split_tuple,
                          \&format_number,
                          \&configurationinfo,
                          \&configurationwarn,
                          \&configurationerror,
                          \&fileerror,
                          getlogger(__PACKAGE__));
        configurationinfo('settings file ' . $configfile . ' loaded',getlogger(__PACKAGE__));
        return $result;
    } else {
        my $result = update_mainconfig($data,$configfile,
                          \&split_tuple,
                          \&format_number,
                          \&configurationinfo,
                          \&configurationwarn,
                          \&configurationerror,
                          \&fileerror,
                          getlogger(__PACKAGE__));
        _splashinfo();
        return $result;
    }

}

sub _splashinfo {

    configurationinfo($system_name . ' ' . $system_version . ' (' . $system_instance_label . ') [' . $local_fqdn . ']',getlogger(__PACKAGE__));
    configurationinfo('application path: ' . $application_path,getlogger(__PACKAGE__));
    configurationinfo('working path: ' . $working_path,getlogger(__PACKAGE__));
    configurationinfo($cpucount . ' cpu(s), multithreading ' . ($enablemultithreading ? 'enabled' : 'disabled'),getlogger(__PACKAGE__));

}

sub _parse_config {
    my ($file,$configtype) = @_;
    my $data;
    if (defined $configtype) {
        if ($configtype == $SIMPLE_CONFIG_TYPE) {
            $data = _parse_simple_config($file);
        } elsif ($configtype == $YAML_CONFIG_TYPE) {
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
        fileerror('parsing simple format - cannot open file ' . $file . ': ' . $!,getlogger(__PACKAGE__));
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
            configurationwarn($file,'parsing simple format - parameter ' . $key . ' defined twice in line ' . $count,getlogger(__PACKAGE__));
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
        configurationerror($file,'parsing yaml format - error: ' . $!,getlogger(__PACKAGE__));
        return $yaml;
    }

    return $yaml;

}

1;
