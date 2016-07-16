package NGCP::BulkProcessor::Globals;
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

use NGCP::BulkProcessor::Utils qw(
    get_ipaddress
    get_hostfqdn
    get_cpucount
    makepath
    fixdirpath
    $chmod_umask
);

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

    create_path
	$appstartsecs
	$enablemultithreading
	$root_threadid
	$cpucount

	$cells_transfer_memory_limit
	$LongReadLen_limit
	$transfer_defer_indexes

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

    $provisioning_databasename
    $provisioning_username
    $provisioning_password
    $provisioning_host
    $provisioning_port

    $kamailio_databasename
    $kamailio_username
    $kamailio_password
    $kamailio_host
    $kamailio_port

    $xa_databasename
    $xa_username
    $xa_password
    $xa_host
    $xa_port

    $ngcprestapi_uri
    $ngcprestapi_username
    $ngcprestapi_password
    $ngcprestapi_realm

	$csv_path

    $local_db_path
    $emailenable
    $erroremailrecipient
    $warnemailrecipient
    $completionemailrecipient
    $doneemailrecipient
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

    update_masterconfig
    create_path

    $chmod_umask

    @jobservers
    $jobnamespace
);

#set process umask for open and mkdir calls:
umask 0000;

# general constants
our $system_name = 'Sipwise Bulk Processing Framework';
our $VERSION = '0.0.1';
our $system_version = $VERSION; #keep this filename-save
our $system_abbreviation = 'sbpf'; #keep this filename-, dbname-save
our $system_instance = 'initial'; #'test'; #'2014'; #dbname-save 0-9a-z_
our $system_instance_label = 'test';

our $local_ip = get_ipaddress();
our $local_fqdn = get_hostfqdn();
our $application_path = get_applicationpath();
our $executable_path = $FindBin::Bin . '/';
#my $remotefilesystem = "MSWin32";
#our $system_username = 'system';

our $enablemultithreading;
if ($^O eq 'MSWin32') {
    $enablemultithreading = 1; # tested ok with windows.
} else {
    $enablemultithreading = 1; # oel 5.4 perl 5.8.8 obvoisly not ok.
}

our $cpucount = get_cpucount();

our $root_threadid = 0; #threadid() . ''; #0
our $cells_transfer_memory_limit = 10000000; #db fields
our $transfer_defer_indexes = 1;
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

our	$provisioning_databasename = 'provisioning';
our $provisioning_username = 'root';
our	$provisioning_password = '';
our $provisioning_host = '127.0.0.1';
our $provisioning_port = '3306';

our	$kamailio_databasename = 'kamailio';
our $kamailio_username = 'root';
our	$kamailio_password = '';
our $kamailio_host = '127.0.0.1';
our $kamailio_port = '3306';

our	$xa_databasename = 'ngcp';
our $xa_username = 'root';
our	$xa_password = '';
our $xa_host = '127.0.0.1';
our $xa_port = '3306';


our $ngcprestapi_uri = 'https://127.0.0.1:443';
our $ngcprestapi_username = 'administrator';
our $ngcprestapi_password = 'administrator';
our $ngcprestapi_realm = 'api_admin_http';

our $working_path = tempdir(CLEANUP => 0) . '/'; #'/var/sipwise/';

#our $input_path = $working_path . 'input/';


our $provisioning_conf = undef;


# csv
our $csv_path = $working_path . 'csv/';
#mkdir $csv_path;

# logging
our $logfile_path = $working_path . 'log/';
#mkdir $logfile_path;

our $fileloglevel = 'OFF'; #'DEBUG';
our $screenloglevel = 'INFO'; #'DEBUG';

our $emailloglevel = 'OFF'; #'INFO';






# local db setup
our $local_db_path = $working_path . 'db/';
#mkdir $local_db_path;


#our $rollback_path = $working_path . 'rollback/';


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
our $doneemailrecipient = '';

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


sub update_masterconfig {

    my %params = @_;
    my ($data,
        $configfile,
        $split_tuplecode,
        $format_numbercode,
        $parse_regexpcode,
        $configurationinfocode,
        $configurationwarncode,
        $configurationerrorcode,
        $fileerrorcode,
        $simpleconfigtype,
        $yamlconfigtype,
        $anyconfigtype,
        $configlogger) = @params{qw/
            data
            configfile
            split_tuplecode
            format_numbercode
            parse_regexpcode
            configurationinfocode
            configurationwarncode
            configurationerrorcode
            fileerrorcode
            simpleconfigtype
            yamlconfigtype
            anyconfigtype
            configlogger
        /};

    if (defined $data) {

        my $result = 1;

        $ngcprestapi_uri = $data->{ngcprestapi_uri} if exists $data->{ngcprestapi_uri};
        $ngcprestapi_username = $data->{ngcprestapi_username} if exists $data->{ngcprestapi_username};
        $ngcprestapi_password = $data->{ngcprestapi_password} if exists $data->{ngcprestapi_password};
        $ngcprestapi_realm = $data->{ngcprestapi_realm} if exists $data->{ngcprestapi_realm};

        $cpucount = $data->{cpucount} if exists $data->{cpucount};
        $enablemultithreading = $data->{enablemultithreading} if exists $data->{enablemultithreading};
        $cells_transfer_memory_limit = $data->{cells_transfer_memory_limit} if exists $data->{cells_transfer_memory_limit};
        $transfer_defer_indexes = $data->{transfer_defer_indexes} if exists $data->{transfer_defer_indexes};


        if (defined $split_tuplecode and ref $split_tuplecode eq 'CODE') {
            @jobservers = &$split_tuplecode($data->{jobservers}) if exists $data->{jobservers};
        } else {
            @jobservers = ($data->{jobservers}) if exists $data->{jobservers};
        }

        if (defined $format_numbercode and ref $format_numbercode eq 'CODE') {

        }

        if (defined $parse_regexpcode and ref $parse_regexpcode eq 'CODE') {

        }


        $emailenable = $data->{emailenable} if exists $data->{emailenable};
        $erroremailrecipient = $data->{erroremailrecipient} if exists $data->{erroremailrecipient};
        $warnemailrecipient = $data->{warnemailrecipient} if exists $data->{warnemailrecipient};
        $completionemailrecipient = $data->{completionemailrecipient} if exists $data->{completionemailrecipient};
        $doneemailrecipient = $data->{doneemailrecipient} if exists $data->{doneemailrecipient};

        $ismsexchangeserver = $data->{ismsexchangeserver} if exists $data->{ismsexchangeserver};
        $smtp_server = $data->{smtp_server} if exists $data->{smtp_server};
        $smtpuser = $data->{smtpuser} if exists $data->{smtpuser};
        $smtppasswd = $data->{smtppasswd} if exists $data->{smtppasswd};

        $fileloglevel = $data->{fileloglevel} if exists $data->{fileloglevel};
        $screenloglevel = $data->{screenloglevel} if exists $data->{screenloglevel};
        $emailloglevel = $data->{emailloglevel} if exists $data->{emailloglevel};

        if (exists $data->{working_path}) {
            $result &= _prepare_working_paths($data->{working_path},1,$fileerrorcode,$configlogger);
        } else {
            $result &= _prepare_working_paths($working_path,1,$fileerrorcode,$configlogger);
        }

        my @loadconfig_args = ();

        $provisioning_conf = $data->{provisioning_conf} if exists $data->{provisioning_conf};

        if (defined $provisioning_conf and length($provisioning_conf) > 0) {
            push(@loadconfig_args,[
                $provisioning_conf,
                \&_update_provisioning_conf,
                $anyconfigtype,
                { force_plugins => [ 'Config::Any::XML' ] }
            ]);
        }

        return ($result,\@loadconfig_args,\&_postprocess_masterconfig);

    }
    return (0,undef,\&_postprocess_masterconfig);

}

sub _update_provisioning_conf {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;



        return $result;

    }
    return 0;

}

sub _postprocess_masterconfig {

    my %params = @_;
    my ($data) = @params{qw/data/};

    if (defined $data) {
            # databases - dsp
        $accounting_host = $data->{accounting_host} if exists $data->{accounting_host};
        $accounting_port = $data->{accounting_port} if exists $data->{accounting_port};
        $accounting_databasename = $data->{accounting_databasename} if exists $data->{accounting_databasename};
        $accounting_username = $data->{accounting_username} if exists $data->{accounting_username};
        $accounting_password = $data->{accounting_password} if exists $data->{accounting_password};

        $billing_host = $data->{billing_host} if exists $data->{billing_host};
        $billing_port = $data->{billing_port} if exists $data->{billing_port};
        $billing_databasename = $data->{billing_databasename} if exists $data->{billing_databasename};
        $billing_username = $data->{billing_username} if exists $data->{billing_username};
        $billing_password = $data->{billing_password} if exists $data->{billing_password};

        $provisioning_host = $data->{provisioning_host} if exists $data->{provisioning_host};
        $provisioning_port = $data->{provisioning_port} if exists $data->{provisioning_port};
        $provisioning_databasename = $data->{provisioning_databasename} if exists $data->{provisioning_databasename};
        $provisioning_username = $data->{provisioning_username} if exists $data->{provisioning_username};
        $provisioning_password = $data->{provisioning_password} if exists $data->{provisioning_password};

        $kamailio_host = $data->{kamailio_host} if exists $data->{kamailio_host};
        $kamailio_port = $data->{kamailio_port} if exists $data->{kamailio_port};
        $kamailio_databasename = $data->{kamailio_databasename} if exists $data->{kamailio_databasename};
        $kamailio_username = $data->{kamailio_username} if exists $data->{kamailio_username};
        $kamailio_password = $data->{kamailio_password} if exists $data->{kamailio_password};

        $xa_host = $data->{xa_host} if exists $data->{xa_host};
        $xa_port = $data->{xa_port} if exists $data->{xa_port};
        $xa_databasename = $data->{xa_databasename} if exists $data->{xa_databasename};
        $xa_username = $data->{xa_username} if exists $data->{xa_username};
        $xa_password = $data->{xa_password} if exists $data->{xa_password};

        return 1;
    }
    return 0;

}

sub _prepare_working_paths {

    my ($new_working_path,$create,$fileerrorcode,$logger) = @_;
    my $result = 1;
    my $path_result;

    ($path_result,$working_path) = create_path($new_working_path,$working_path,$create,$fileerrorcode,$logger);
    $result &= $path_result;
    ($path_result,$csv_path) = create_path($working_path . 'csv',$csv_path,$create,$fileerrorcode,$logger);
    $result &= $path_result;
    #($path_result,$input_path) = create_path($working_path . 'input',$input_path,$create,$fileerrorcode,$logger);
    #$result &= $path_result;
    ($path_result,$logfile_path) = create_path($working_path . 'log',$logfile_path,$create,$fileerrorcode,$logger);
    $result &= $path_result;
    ($path_result,$local_db_path) = create_path($working_path . 'db',$local_db_path,$create,$fileerrorcode,$logger);
    $result &= $path_result;
    ($path_result,$mailfile_path) = create_path($working_path . 'mails',$local_db_path,$create,$fileerrorcode,$logger);
    $result &= $path_result;
    #($path_result,$rollback_path) = create_path($working_path . 'rollback',$rollback_path,$create,$fileerrorcode,$logger);
    #$result &= $path_result;

    return $result;

}

sub get_applicationpath {

  return dirname(abs_path(__FILE__)) . '/';

}

sub create_path {
    my ($new_value,$old_value,$create,$fileerrorcode,$logger) = @_;
    my $path = $old_value;
    my $result = 0;
    if (defined $new_value and length($new_value) > 0) {
        $new_value = fixdirpath($new_value);
        if (-d $new_value) {
            $path = $new_value;
            $result = 1;
        } else {
            if ($create) {
                if (makepath($new_value,$fileerrorcode,$logger)) {
                    $path = $new_value;
                    $result = 1;
                }
            } else {
                if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                    &$fileerrorcode("path '$new_value' does not exist",$logger);
                }
            }
        }
    } else {
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
            &$fileerrorcode("empty path",$logger);
        }
    }
    return ($result,$path);
}

1;
