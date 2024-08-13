package NGCP::BulkProcessor::LoadConfig;
use strict;

## no critic

use Cwd 'abs_path';

use NGCP::BulkProcessor::Globals qw(
    $system_name
    $system_instance_label
    $local_fqdn
    get_application_version
    $application_path
    $working_path
    $executable_path
    $cpucount
    $enablemultithreading
    $is_perl_debug
    update_masterconfig
    @config_search_paths
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

use YAML qw();
$YAML::UseCode = 1;

use Config::Any qw();
use NGCP::BulkProcessor::Utils qw(format_number trim);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    load_config
    parse_regexp
    split_tuple
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
    $ANY_CONFIG_TYPE
);

my $tuplesplitpattern = join('|',(quotemeta(','),
                                  quotemeta(';'),
                                  #quotemeta('/')
                                  )
                             );

our $SIMPLE_CONFIG_TYPE = 1;
our $YAML_CONFIG_TYPE = 2;
our $ANY_CONFIG_TYPE = 3;
#my $logger = getlogger(__PACKAGE__);

my $debug_config_ext_prefix = 'debug';

sub load_config {

    my ($configfile,$process_code,$configtype,$configparser_args) = @_;

    my $is_master = 'CODE' ne ref $process_code;
    my $data;
    my $variant = $configfile;
    if (defined $configfile) {
        my @variants = ();
        if ($is_perl_debug) {
            push(@variants,_prefix_ext($configfile,$debug_config_ext_prefix));
        }
        push(@variants,$configfile);
        my %dupes = ();
        while (not defined $data and ($variant = shift @variants)) {
            next if exists $dupes{$variant};
            $dupes{$variant} = 1;

            if (-e $variant) {
                $data = _parse_config($variant,$configtype,$configparser_args);
            } else {
                my @paths = ();
                my %path_dupes = ();
                my @search_paths = (@config_search_paths,$executable_path,$application_path); #todo: add /etc/bulkprocessor or similar here once
                ($variant,$data) = _search_path($variant,$configtype,$configparser_args,\@search_paths,\@paths,\%path_dupes);
                @search_paths = ();
                if (not defined $data) {
                    if (index($executable_path,$application_path) > -1) {
                        my $module_path  = 'NGCP/BulkProcessor/' . substr($executable_path,length($application_path));
                        push(@search_paths,map { eval{ Cwd::abs_path($_  . '/') . '/' . $module_path; }; } @INC);
                    }
                    push(@search_paths,map { eval{ Cwd::abs_path($_  . '/') . '/'; }; } @INC);
                    ($variant,$data) = _search_path($variant,$configtype,$configparser_args,\@search_paths,\@paths,\%path_dupes);
                }
            }
        }
        if (not defined $data) {
            configurationerror($configfile,'no ' . ($is_master ? 'master config' : 'config') . ' variant found',getlogger(__PACKAGE__));
        }
    } else {
        fileerror('no ' . ($is_master ? 'master config' : 'config') . ' file specified',getlogger(__PACKAGE__));
        return 0;
    }

    if ($is_master) {
        my %context = (
            data => $data,
            configfile => $variant,
            split_tuplecode => \&split_tuple,
            format_numbercode => \&format_number,
            parse_regexpcode => \&parse_regexp,
            configurationinfocode => \&configurationinfo,
            configurationwarncode => \&configurationwarn,
            configurationerrorcode => \&configurationerror,
            fileerrorcode => \&fileerror,
            simpleconfigtype => $SIMPLE_CONFIG_TYPE,
            yamlconfigtype => $YAML_CONFIG_TYPE,
            anyconfigtype => $ANY_CONFIG_TYPE,
            configlogger => getlogger(__PACKAGE__),
        );
        my ($result,$loadconfig_args,$postprocesscode) = update_masterconfig(%context);
        _splashinfo($variant);
        if (defined $loadconfig_args and 'ARRAY' eq ref $loadconfig_args) {
            foreach my $loadconfig_arg (@$loadconfig_args) {
                $result &= load_config(@$loadconfig_arg);
            }
        }
        if (defined $postprocesscode and 'CODE' eq ref $postprocesscode) {
            $result &= &$postprocesscode(%context);
        }
        return $result;
    } else {
        my $result = &$process_code($data,$variant);
        my $msg = 'config file ' . $variant . ' loaded';
        $msg .= ' (' . abs_path($variant) . ')' if $variant ne abs_path($variant);
        configurationinfo($msg,getlogger(__PACKAGE__));
        return $result;
    }

}

sub _prefix_ext {
    my ($configfile,$ext_suffix) = @_;
    return $configfile unless $ext_suffix;
    if ($configfile =~ /\.([^\.]+)$/) {
        $configfile =~ s/\.([^\.]+)$/.$ext_suffix.$1/;
    } else {
        $configfile .= '.' . $ext_suffix;
    }
    return $configfile;
}

sub _search_path {

    my ($configfile,$configtype,$configparser_args,$search_paths,$paths,$dupes) = @_;
    my $data = undef;
    $dupes //= {};
    while (not defined $data and (my $path = shift @$search_paths)) {
        next if exists $dupes->{$path};
        push(@$paths,$path);
        $dupes->{$path} = 1;
        my $relative_configfile = $path . $configfile;
        if (-e $relative_configfile) {
            $configfile = $relative_configfile;
            $data = _parse_config($configfile,$configtype,$configparser_args);
        #} else {
        #    configurationwarn($configfile,'no ' . ($is_master ? 'master config' : 'config') . ' file ' . $relative_configfile,getlogger(__PACKAGE__));
        }
    }
    return ($configfile,$data);

}

sub _splashinfo {

    my ($configfile) = @_;
    configurationinfo($system_name . (length($system_instance_label) ? ' (' . $system_instance_label . ')' : '') . ' [' . $local_fqdn . ']',getlogger(__PACKAGE__));
    configurationinfo('application version: ' . get_application_version(),getlogger(__PACKAGE__));
    configurationinfo('application path: ' . $application_path,getlogger(__PACKAGE__));
    configurationinfo('working path: ' . $working_path,getlogger(__PACKAGE__));
    configurationinfo($cpucount . ' cpu(s), multithreading ' . ($enablemultithreading ? 'enabled' : 'disabled'),getlogger(__PACKAGE__));
    my $msg = 'master config file ' . $configfile . ' loaded';
    $msg .= ' (' . abs_path($configfile) . ')' if $configfile ne abs_path($configfile);
    configurationinfo($msg,getlogger(__PACKAGE__));
    configurationinfo('WARNING: running perl debug',getlogger(__PACKAGE__)) if $is_perl_debug;

}

sub _parse_config {
    my ($file,$configtype,$configparser_args) = @_;
    my $data;
    if (defined $configtype) {
        if ($configtype == $SIMPLE_CONFIG_TYPE) {
            $data = _parse_simple_config($file,$configparser_args);
        } elsif ($configtype == $YAML_CONFIG_TYPE) {
            $data = _parse_yaml_config($file,$configparser_args);
        } elsif ($configtype == $ANY_CONFIG_TYPE) {
            $data = _parse_any_config($file,$configparser_args);
        } else {
            $data = _parse_simple_config($file,$configparser_args);
        }
    } else {
        $data = _parse_simple_config($file,$configparser_args);
    }
    return $data;
}

sub split_tuple {

    my $token = shift;
    return map { local $_ = $_; trim($_); } split(/$tuplesplitpattern/,$token);

}

sub parse_regexp {

    my ($token,$file) = @_;
    my $regexp = undef;
    my $result = 1;
    if (defined $token and length($token) > 0) {
        eval {
            $regexp = qr/$token/;
        };
        if ($@ or !defined $regexp) {
            configurationerror($file,'invalid pattern: ' . $@,getlogger(__PACKAGE__));
            $result = 0;
        }
    }
    return ($result,$regexp);

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

    my ($file,$configparser_args) = @_;

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

    my ($file,$configparser_args) = @_;

    my $config;
    unless (-e $file and -f _ and -r _) {
        filewarn('parsing yaml format - cannot open file ' . $file,getlogger(__PACKAGE__));
        return $config;
    }

    eval {
        $config = YAML::LoadFile($file) // {};
    };
    if ($@) {
        configurationerror($file,'parsing yaml format - error: ' . $@,getlogger(__PACKAGE__));
    }

    return $config;

}

sub _parse_any_config {

    my ($file,$configparser_args) = @_;

    my $config;

    unless (-e $file and -f _ and -r _) {
        filewarn('parsing any format - cannot open file ' . $file,getlogger(__PACKAGE__));
        return $config;
    }

    eval {
        $config = Config::Any->load_files( { files => [ $file ], (defined $configparser_args ? %$configparser_args : ()) } ) // {};
    };
    if ($@) {
        configurationerror($file,'parsing any format - error: ' . $@,getlogger(__PACKAGE__));
    }

    return $config;

}

1;
