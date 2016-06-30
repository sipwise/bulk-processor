package Globals;
use strict;

## no critic

use 5.8.8;

use threads; # as early as possible...
use threads::shared;

use Time::HiRes qw(time);

use Tie::IxHash;

use Cwd 'abs_path'; 
use File::Basename qw(dirname);
use File::Temp qw(tempdir);
use FindBin qw();

use Utils qw(
	get_ipaddress
  get_hostfqdn
  get_cpucount
  makepath
  fixdirpath
  $chmod_umask);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
	$system_name 
	$system_version 
	$system_abbreviation 
	$system_instance 
	$system_instance_label 
	$local_ip
	$local_fqdn
	$application_path
	$executable_path
	$working_path
	update_working_path
	$appstartsecs
	$enablemultithreading
	$root_threadid
	$cpucount
                    
	$cells_transfer_memory_limit
	$LongReadLen_limit
	$defer_indexes

	$accounting_databasename
	$accounting_username
	$accounting_password
	$accounting_host
	$accounting_port
	
	$billing_databasename
	$billing_username
	$billing_password
	$billing_host
	$billing_port	

$ngcprestapi_uri
$ngcprestapi_username
$ngcprestapi_password
$ngcprestapi_realm

	$csv_path
	$input_path
                    

                    $local_db_path
                    $emailenable
                    $erroremailrecipient
                    $warnemailrecipient
                    $completionemailrecipient
                    $successemailrecipient               
                    $mailfile_path

                    $ismsexchangeserver
                    $sender_address
                    $smtp_server
                    $smtpuser
                    $smtppasswd
                    $writefiles

                    $logfile_path
                    $fileloglevel
                    $screenloglevel
                    $emailloglevel
$mailprog
$mailtype

                    
                    $defaultconfig

                    update_mainconfig
                    log_mainconfig
                    
                    
                    $chmod_umask
                    
                    @jobservers 
                    $jobnamespace
                    
                    
                    
                    );


#set process umask for open and mkdir calls:
umask 0000;

# general constants
our $system_name = 'Sipwise Bulk Processing Framework';
our $system_version = '0.0.1'; #keep this filename-save
our $system_abbreviation = 'sbpf'; #keep this filename-, dbname-save
our $system_instance = 'initial'; #'test'; #'2014'; #dbname-save 0-9a-z_
our $system_instance_label = 'test'; 

our $local_ip = get_ipaddress();
our $local_fqdn = get_hostfqdn();
our $application_path = get_applicationpath();
our $executable_path = $FindBin::Bin . '/';
#my $remotefilesystem = "MSWin32";
our $system_username = 'system';

our $enablemultithreading;
if ($^O eq 'MSWin32') {
    $enablemultithreading = 1; # tested ok with windows.
} else {
    $enablemultithreading = 1; # oel 5.4 perl 5.8.8 obvoisly not ok.
}

our $cpucount = get_cpucount();

our $root_threadid = 0; #threadid() . ''; #0
our $cells_transfer_memory_limit = 10000000; #db fields
our $defer_indexes = 1;
#http://docstore.mik.ua/orelly/linux/dbi/ch06_01.htm
our $LongReadLen_limit = 128*1024; #longest LOB field size in bytes

our $appstartsecs = Time::HiRes::time();




our	$accounting_databasename = 'accounting';
our $accounting_username = 'root';
our	$accounting_password = '';
our $accounting_host = '127.0.0.1';
our $accounting_port = '3306';

our	$billing_databasename = 'billing';
our $billing_username = 'root';
our	$billing_password = '';
our $billing_host = '127.0.0.1';
our $billing_port = '3306';

our $ngcprestapi_uri = 'https://127.0.0.1:443';
our $ngcprestapi_username = 'administrator';
our $ngcprestapi_password = 'administrator';
our $ngcprestapi_realm = 'api_admin_http';

our $working_path = tempdir(CLEANUP => 0) . '/'; #'/var/sipwise/';

our $input_path = $working_path . 'input/';

# csv
our $csv_path = $working_path . 'csv/';
#mkdir $csv_path;

# logging
our $logfile_path = $working_path . 'log/';
#mkdir $logfile_path;

our $fileloglevel = 'OFF'; #'DEBUG';
our $screenloglevel = 'OFF'; #'DEBUG';

our $emailloglevel = 'OFF'; #'INFO';






# local db setup
our $local_db_path = $working_path . 'db/';
#mkdir $local_db_path;





# email setup
#set emailenable and writefiles to 0 during development with IDE that perform
#on-the-fly compilation during typing
our $emailenable = 0;                                # globally enable email sending
our $mailfile_path = $working_path . 'mails/';   # emails can be saved (logged) as message files to this folder
#mkdir $mailfilepath;
our $writefiles = 0;                                 # save emails

our $erroremailrecipient = ''; #'rkrenn@sipwise.com';
our $warnemailrecipient = ''; #'rkrenn@sipwise.com';
our $completionemailrecipient = '';
our $successemailrecipient = '';

our $mailprog = "/usr/sbin/sendmail"; # linux only
our $mailtype = 1; #0 .. mailprog, 1 .. socket, 2 .. Net::SMTP


our $ismsexchangeserver = 0;                         # smtp server is a ms exchange server
our $smtp_server = '192.168.0.99';                   # smtp sever ip/hostname
our $smtpuser = 'WORKGROUP\rkrenn';
our $smtppasswd = 'xyz';
our $sender_address = 'donotreply@sipwise.com';



#service layer:
our @jobservers = ('127.0.0.1:4730');
#our $jobnamespace = $system_abbreviation . '-' . $system_version . '-' . $local_fqdn . '-' . $system_instance;
our $jobnamespace = $system_abbreviation . '-' . $system_version . '-' . $system_instance;



# test directory
#our $tpath = $application_path . 't/';
#mkdir $tpath;



our $defaultconfig = 'default.cfg';


sub update_mainconfig {
    
    my ($config,$configfile,
        $split_tuplecode,
        $parse_floatcode,
        $configurationwarncode,
        $configurationerrorcode,
        $fileerrorcode,
        $configlogger) = @_;
    
    if (defined $config) {

        # databases - dsp
        $accounting_host = $config->{accounting_host} if exists $config->{accounting_host};
        $accounting_port = $config->{accounting_port} if exists $config->{accounting_port};
        $accounting_databasename = $config->{accounting_databasename} if exists $config->{accounting_databasename};
        $accounting_username = $config->{accounting_username} if exists $config->{accounting_username};
        $accounting_password = $config->{accounting_password} if exists $config->{accounting_password};
        
        $billing_host = $config->{billing_host} if exists $config->{billing_host};
        $billing_port = $config->{billing_port} if exists $config->{billing_port};
        $billing_databasename = $config->{billing_databasename} if exists $config->{billing_databasename};
        $billing_username = $config->{billing_username} if exists $config->{billing_username};
        $billing_password = $config->{billing_password} if exists $config->{billing_password};

        $ngcprestapi_uri = $config->{ngcprestapi_uri} if exists $config->{ngcprestapi_uri};
        $ngcprestapi_username = $config->{ngcprestapi_username} if exists $config->{ngcprestapi_username};
        $ngcprestapi_password = $config->{ngcprestapi_password} if exists $config->{ngcprestapi_password};        
        $ngcprestapi_realm = $config->{ngcprestapi_realm} if exists $config->{ngcprestapi_realm};        
        
        $enablemultithreading = $config->{enablemultithreading} if exists $config->{enablemultithreading};
        $cells_transfer_memory_limit = $config->{cells_transfer_memory_limit} if exists $config->{cells_transfer_memory_limit};
        $defer_indexes = $config->{defer_indexes} if exists $config->{defer_indexes};
        
        
        if (defined $split_tuplecode and ref $split_tuplecode eq 'CODE') {
            @jobservers = &$split_tuplecode($config->{jobservers}) if exists $config->{jobservers};
        } else {
            @jobservers = ($config->{jobservers}) if exists $config->{jobservers};
        }
        
        if (defined $parse_floatcode and ref $parse_floatcode eq 'CODE') {

        }

        
        $emailenable = $config->{emailenable} if exists $config->{emailenable};
        $erroremailrecipient = $config->{erroremailrecipient} if exists $config->{erroremailrecipient};
        $warnemailrecipient = $config->{warnemailrecipient} if exists $config->{warnemailrecipient};
        $completionemailrecipient = $config->{completionemailrecipient} if exists $config->{completionemailrecipient};
        $successemailrecipient = $config->{successemailrecipient} if exists $config->{successemailrecipient};
        
        $ismsexchangeserver = $config->{ismsexchangeserver} if exists $config->{ismsexchangeserver};
        $smtp_server = $config->{smtp_server} if exists $config->{smtp_server};
        $smtpuser = $config->{smtpuser} if exists $config->{smtpuser};
        $smtppasswd = $config->{smtppasswd} if exists $config->{smtppasswd};
        
        $fileloglevel = $config->{fileloglevel} if exists $config->{fileloglevel};
        $screenloglevel = $config->{screenloglevel} if exists $config->{screenloglevel};
        $emailloglevel = $config->{emailloglevel} if exists $config->{emailloglevel};
        
        my $new_working_path = (exists $config->{working_path} ? $config->{working_path} : $working_path);

        return update_working_path($new_working_path,1,$fileerrorcode,$configlogger);
        
    }
    return 0;
    
}

sub update_working_path {
    
    my ($new_working_path,$create,$fileerrorcode,$logger) = @_;
    my $result = 1;
    if (defined $new_working_path and length($new_working_path) > 0) {
        $new_working_path = fixdirpath($new_working_path);
        if (-d $new_working_path) {
            $working_path = $new_working_path;
        } else {
            if ($create) {
                if (makepath($new_working_path,$fileerrorcode,$logger)) {
                    $working_path = $new_working_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("working path '$new_working_path' does not exist",$logger);
                }
            }
        }
        
        my $new_csv_path = $working_path . 'csv/';
        if (-d $new_csv_path) {
            $csv_path = $new_csv_path;
        } else {
            if ($create) {
                if (makepath($new_csv_path,$fileerrorcode,$logger)) {
                    $csv_path = $new_csv_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("csv path '$new_csv_path' does not exist",$logger);
                }
            }
        }
        
        my $new_input_path = $working_path . 'input/';
        if (-d $new_input_path) {
            $input_path = $new_input_path;
        } else {
            if ($create) {
                if (makepath($new_input_path,$fileerrorcode,$logger)) {
                    $input_path = $new_input_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("input path '$new_input_path' does not exist",$logger);
                }
            }
        }
        
        my $new_logfile_path = $working_path . 'log/';
        if (-d $new_logfile_path) {
            $logfile_path = $new_logfile_path;
        } else {
            if ($create) {
                if (makepath($new_logfile_path,$fileerrorcode,$logger)) {
                    $logfile_path = $new_logfile_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("logfile path '$new_logfile_path' does not exist",$logger);
                }
            }
        }        
        
        my $new_local_db_path = $working_path . 'db/';
        if (-d $new_local_db_path) {
            $local_db_path = $new_local_db_path;
        } else {
            if ($create) {
                if (makepath($new_local_db_path,$fileerrorcode,$logger)) {
                    $local_db_path = $new_local_db_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("local db path '$new_local_db_path' does not exist",$logger);
                }
            }
        }  

        my $new_mailfile_path = $working_path . 'mails/';
        if (-d $new_mailfile_path) {
            $mailfile_path = $new_mailfile_path;
        } else {
            if ($create) {
                if (makepath($new_mailfile_path,$fileerrorcode,$logger)) {
                    $mailfile_path = $new_mailfile_path;
                } else {
                    $result = 0;
                }
            } else {
                $result = 0;
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("mailfile path '$new_mailfile_path' does not exist",$logger);
                }
            }
        }
        
    } else {
        $result = 0;
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
            &$fileerrorcode("empty working path",$logger);
        }  
    }
    return $result;

}

sub log_mainconfig {
    
    my ($logconfigcode,$configlogger) = @_;
    if (defined $logconfigcode and ref $logconfigcode eq 'CODE') {
        &$logconfigcode($system_name . ' ' . $system_version . ' (' . $system_instance_label . ') [' . $local_fqdn . ']',$configlogger);
        &$logconfigcode('application path ' . $application_path,$configlogger);
        &$logconfigcode('working path ' . $working_path,$configlogger);
        #&$logconfigcode('executable path ' . $executable_path,$configlogger);
        &$logconfigcode($cpucount . ' cpu(s), multithreading ' . ($enablemultithreading ? 'enabled' : 'disabled'),$configlogger);
    }
    
}


sub get_applicationpath {

  return dirname(abs_path(__FILE__)) . '/';

}

1;

