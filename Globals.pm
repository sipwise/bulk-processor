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

use Utils qw(
	get_ipaddress
  get_hostfqdn
  get_cpucount
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

	$csv_path
                    

                    $local_db_path
                    $emailenable
                    $erroremailrecipient
                    $warnemailrecipient
                    $completionemailrecipient
                    $successemailrecipient               
                    $mailfilepath

                    $ismsexchangeserver
                    $sender_address
                    $smtp_server
                    $smtpuser
                    $smtppasswd
                    $writefiles
                    $tpath

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
umask oct($chmod_umask);

# general constants
our $system_name = 'Sipwise Bulk Processor Framework';
our $system_version = '0.0.1'; #keep this filename-save
our $system_abbreviation = 'bpf'; #keep this filename-, dbname-save
our $system_instance = 'initial'; #'test'; #'2014'; #dbname-save 0-9a-z_
our $system_instance_label = 'test'; 

our $local_ip = get_ipaddress();
our $local_fqdn = get_hostfqdn();
our $application_path = get_applicationpath();
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

our	$billing_databasename = 'accounting';
our $billing_username = 'root';
our	$billing_password = '';
our $billing_host = '127.0.0.1';
our $billing_port = '3306';


# csv
our $csv_path = $application_path . 'csv/';
#mkdir $csv_path;

# logging
our $logfile_path = $application_path . 'log/';
#mkdir $logfile_path;

our $fileloglevel = 'OFF'; #'DEBUG';
our $screenloglevel = 'OFF'; #'DEBUG';

our $emailloglevel = 'OFF'; #'INFO';






# local db setup
our $local_db_path = $application_path . 'db/';
#mkdir $local_db_path;





# email setup
#set emailenable and writefiles to 0 during development with IDE that perform
#on-the-fly compilation during typing
our $emailenable = 0;                                # globally enable email sending
our $mailfilepath = $application_path . 'mails/';   # emails can be saved (logged) as message files to this folder
#mkdir $mailfilepath;
our $writefiles = 0;                                 # save emails

our $erroremailrecipient = ''; #'rkrenn@sipwise.com';
our $warnemailrecipient = ''; #'rkrenn@sipwise.com';
our $completionemailrecipient = '';
our $successemailrecipient = '';

our $mailprog = "/usr/sbin/sendmail"; # linux only
our $mailtype = 1; #0 .. mailprog, 1 .. socket, 2 .. Net::SMTP


our $ismsexchangeserver = 0;                         # smtp server is a ms exchange server
our $smtp_server = '10.146.1.17';                    # smtp sever ip/hostname
our $smtpuser = 'WORKGROUP\rkrenn';
our $smtppasswd = 'xyz';
our $sender_address = 'donotreply@sipwise.com';



#service layer:
our @jobservers = ('127.0.0.1:4730');
#our $jobnamespace = $system_abbreviation . '-' . $system_version . '-' . $local_fqdn . '-' . $system_instance;
our $jobnamespace = $system_abbreviation . '-' . $system_version . '-' . $system_instance;



# test directory
our $tpath = $application_path . 't/';
#mkdir $tpath;



our $defaultconfig = 'default.cfg';


sub update_mainconfig {
    
    my ($config,$configfile,
        $split_tuplecode,
        $parse_floatcode,
        $configurationwarncode,
        $configurationerrorcode,
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
        
        return 1;
        
    }
    return 0;
    
}

sub log_mainconfig {
    
    my ($logconfigcode,$configlogger) = @_;
    if (defined $logconfigcode and ref $logconfigcode eq 'CODE') {
        &$logconfigcode($system_name . ' ' . $system_version . ' (' . $system_instance_label . ') [' . $local_fqdn . ']',$configlogger);
        &$logconfigcode('application path ' . $application_path,$configlogger);
        &$logconfigcode($cpucount . ' cpu(s), multithreading ' . ($enablemultithreading ? 'enabled' : 'disabled'),$configlogger);
    }
    
}


sub get_applicationpath {

  return dirname(abs_path(__FILE__)) . '/';

}

1;

