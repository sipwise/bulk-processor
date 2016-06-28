# a verbose logging module

package Logging;
use strict;

## no critic

use Globals qw(
    $root_threadid
    $logfile_path
    $fileloglevel
    $emailloglevel
    $screenloglevel
    log_mainconfig
    $enablemultithreading
);

use Log::Log4perl qw(get_logger);

use Utils qw(timestampdigits datestampdigits changemod chopstring trim);
use Array qw (contains);

require Exporter;
our @ISA = qw(Exporter);

our @EXPORT_OK = qw(
    getlogger
    
    cleanuplogfiles

    emailinfo
    emaildebug
    dbdebug
    dbinfo
    
	attachmentdownloaderdebug
    attachmentdownloaderinfo  

    fieldnamesaquired
    primarykeycolsaquired
    tableinfoscleared
    tabletransferstarted
    tableprocessingstarted
    rowtransferstarted
    texttablecreated
    temptablecreated
    indexcreated
    primarykeycreated
    tabletruncated
    tabledropped
    rowtransferred
    rowskipped
    rowinserted
    rowupdated
    rowsdeleted
    totalrowsdeleted
    rowinsertskipped
    rowupdateskipped
    tabletransferdone
    tableprocessingdone
    rowtransferdone
    fetching_rows
    writing_rows
    processing_rows

    mainconfigurationloaded
    configinfo
    init_log
    $currentlogfile
    $attachmentlogfile
    cleanupinfo

    xls2csvinfo
    tablethreadingdebug

    tablefixed
    servicedebug
    serviceinfo
);
#abortthread

my $logfileextension = '.log';

our $currentlogfile;
#our $weblogfile;
our $attachmentlogfile;

my $loginitialized = 0;

##eval {
#    init_log4perl();
##};
init_log_default();

sub createlogfile {

    my ($logfile,$fileerrorcode,$logger) = @_;
    local *LOGFILE;
    if (not open (LOGFILE,'>' . $logfile)) {
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
                &$fileerrorcode('cannot create log file ' . $logfile . ': ' . $!,$logger);
            }
    }
    close(LOGFILE);
    changemod($logfile);

}

sub init_log_default {

    # log configuration
    my $conf = "log4perl.logger                       = DEBUG, ScreenApp\n" .

               "log4perl.appender.ScreenApp           = Log::Log4perl::Appender::Screen\n" .
               #"log4perl.appender.ScreenApp           = Log::Log4perl::Appender::ScreenColoredLevels\n" .
               "log4perl.appender.ScreenApp.Threshold = INFO\n" .
               "log4perl.appender.ScreenApp.stderr    = 0\n" .
               "log4perl.appender.ScreenApp.layout    = Log::Log4perl::Layout::SimpleLayout\n" .
               'log4perl.appender.ScreenApp.layout.ConversionPattern = %d> %m%n';

    # Initialize logging behaviour
    Log::Log4perl->init( \$conf );
    get_logger(__PACKAGE__)->debug('default log4perl configuration applied');

}

sub init_log {

    $currentlogfile = $logfile_path . timestampdigits() . $logfileextension;
    createlogfile($currentlogfile);
    $attachmentlogfile = $logfile_path . 'email_' . timestampdigits() . $logfileextension;
    createlogfile($attachmentlogfile);
    #$weblogfile = $logfile_path . 'web_' . datestampdigits() .  $logfileextension;
    #createlogfile($weblogfile);

    # log configuration
    my $conf = "log4perl.logger                       = DEBUG, FileApp, ScreenApp, MailAttApp\n" .

               "log4perl.appender.FileApp             = Log::Log4perl::Appender::File\n" .
               "log4perl.appender.FileApp.umask       = 0\n" .
               "log4perl.appender.FileApp.syswite     = 1\n" .
               'log4perl.appender.FileApp.Threshold   = ' . $fileloglevel . "\n" .
               "log4perl.appender.FileApp.mode        = append\n" .
               'log4perl.appender.FileApp.filename    = ' . $currentlogfile . "\n" .
               "log4perl.appender.FileApp.create_at_logtime = 1\n" .
               "log4perl.appender.FileApp.layout      = PatternLayout\n" .
               'log4perl.appender.FileApp.layout.ConversionPattern = %d> %m%n' . "\n\n" .

               "log4perl.appender.MailAttApp             = Log::Log4perl::Appender::File\n" .
               "log4perl.appender.MailApp.umask          = 0\n" .
               "log4perl.appender.MailApp.syswite        = 1\n" .
               'log4perl.appender.MailAttApp.Threshold   = ' . $emailloglevel . "\n" .
               "log4perl.appender.MailAttApp.mode        = append\n" .
               'log4perl.appender.MailAttApp.filename    = ' . $attachmentlogfile . "\n" .
               "log4perl.appender.MailAttApp.create_at_logtime = 1\n" .
               "log4perl.appender.MailAttApp.layout      = Log::Log4perl::Layout::SimpleLayout\n" .
               'log4perl.appender.MailAttApp.layout.ConversionPattern = %d> %m%n' . "\n\n" .

               "log4perl.appender.ScreenApp           = Log::Log4perl::Appender::Screen\n" .
               #"log4perl.appender.ScreenApp           = Log::Log4perl::Appender::ScreenColoredLevels\n" .
               'log4perl.appender.ScreenApp.Threshold = ' . $screenloglevel . "\n" .
               "log4perl.appender.ScreenApp.stderr    = 0\n" .
               "log4perl.appender.ScreenApp.layout    = Log::Log4perl::Layout::SimpleLayout\n" .
               'log4perl.appender.ScreenApp.layout.ConversionPattern = %d> %m%n';

    # Initialize logging behaviour
    Log::Log4perl->init( \$conf );
    
    $loginitialized = 1;
    
    get_logger(__PACKAGE__)->debug('log4perl configuration loaded');
}

#my $loglogger;
#eval {
#    $loglogger = get_logger(__PACKAGE__);
#};
#my $loglogger = undef; #deferred initialisation required, as log options are loaded from configs

#sub _get_loglogger {
#    return get_logger(__PACKAGE__);
#    #if (!defined $loglogger) {
#    #    $loglogger = get_logger(__PACKAGE__);
#    #}
#    #return $loglogger;
#}

sub getlogger {

    my $package = shift;
    #my $newlogger;
    #eval {
    #    $newlogger = get_logger($package);
    #};
    #if (defined $loglogger and defined $newlogger) {
    #    $loglogger->debug('logger for category ' . $package . ' created');
    #}
    return get_logger($package);
    #_get_loglogger()->debug('logger for category ' . $package . ' created');
    #return $newlogger;

}

sub cleanuplogfiles {

    my ($fileerrorcode,$filewarncode,@remaininglogfiles) = @_;
    my $rlogfileextension = quotemeta($logfileextension);
    local *LOGDIR;
    if (not opendir(LOGDIR, $logfile_path)) {
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
          &$fileerrorcode('cannot opendir ' . $logfile_path . ': ' . $!,get_logger(__PACKAGE__));
          return;
        }
    }
    my @files = grep { /$rlogfileextension$/ && -f $logfile_path . $_} readdir(LOGDIR);
    closedir LOGDIR;
    foreach my $file (@files) {
        #print $file;
        my $filepath = $logfile_path . $file;
        #print $filepath . "\n";
        #print $remaininglogfiles[0] . "\n\n";
        if (not contains($filepath,\@remaininglogfiles)) {
            if ((unlink $filepath) == 0) {
                if (defined $filewarncode and ref $filewarncode eq 'CODE') {
                  &$filewarncode('cannot remove ' . $filepath . ': ' . $!,get_logger(__PACKAGE__));
                }
            }
        }
    }

}

sub emailinfo {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub emaildebug {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}

sub dbdebug {

    my ($db, $message, $logger) = @_;
    if (defined $logger) {
        $logger->debug(_getconnectorinstanceprefix($db) . _getconnectidentifiermessage($db,$message));
    }

    #die();

}

sub dbinfo {

    my ($db, $message, $logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . _getconnectidentifiermessage($db,$message));
    }

    #die();

}

sub attachmentdownloaderdebug {
    
    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }
    
}

sub attachmentdownloaderinfo {
    
    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }
    
}

sub xls2csvinfo {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub fieldnamesaquired {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'fieldnames aquired and OK: [' . $db->connectidentifier() . '].' . $tablename);
    }

}

sub primarykeycolsaquired {

    my ($db,$tablename,$keycols,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'primary key columns aquired for [' . $db->connectidentifier() . '].' . $tablename . ': ' . ((defined $keycols and scalar @$keycols > 0) ? join(', ',@$keycols) : '<no primary key columns>'));
    }

}

sub tableinfoscleared {

    my ($db,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'table infos cleared for ' . $db->connectidentifier());
    }

}

sub tabletransferstarted {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'table transfer started: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub tableprocessingstarted {

    my ($db,$tablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info('table processing started: [' . $db->connectidentifier() . '].' . $tablename . ': ' . $numofrows . ' row(s)');
    }

}

sub tablethreadingdebug {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}

sub rowtransferstarted {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'row transfer started: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub texttablecreated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'text table created: ' . $tablename);
    }

}

sub indexcreated {

    my ($db,$tablename,$indexname,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'index created: ' . $indexname . ' on ' . $tablename);
    }

}

sub primarykeycreated {

    my ($db,$tablename,$keycols,$logger) = @_;
    if (defined $logger and (defined $keycols and scalar @$keycols > 0)) {
        $logger->info(_getconnectorinstanceprefix($db) . 'primary key created: ' . join(', ',@$keycols) . ' on ' . $tablename);
    }

}

sub temptablecreated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'temporary table created: ' . $tablename);
    }

}

sub tabletruncated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'table truncated: ' . $tablename);
    }

}

sub tabledropped {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'table dropped: ' . $tablename);
    }

}

sub rowtransferred {

    my ($db,$tablename,$target_db,$targettablename,$i,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getconnectorinstanceprefix($db) . 'row ' . $i . '/' . $numofrows . ' transferred');
    }

}

sub rowskipped {

    my ($db,$tablename,$target_db,$targettablename,$i,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'row ' . $i . '/' . $numofrows . ' skipped');
    }

}

sub rowinserted {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getconnectorinstanceprefix($db) . 'row inserted');
    }

}

sub rowupdated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getconnectorinstanceprefix($db) . 'row updated');
    }

}

sub rowsdeleted {

    my ($db,$tablename,$rowcount,$initial_rowcount,$logger) = @_;
    if (defined $logger) {
    if (defined $initial_rowcount) {
        $logger->debug(_getconnectorinstanceprefix($db) . $rowcount . ' of ' . $initial_rowcount . ' row(s) deleted');
    } else {
        $logger->debug(_getconnectorinstanceprefix($db) . $rowcount . ' row(s) deleted');
    }
    }

}

sub totalrowsdeleted {

    my ($db,$tablename,$rowcount_total,$initial_rowcount,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . $rowcount_total . ' of ' . $initial_rowcount . ' row(s) deleted from [' . $db->connectidentifier() . '].' . $tablename);
    }

}

sub rowinsertskipped {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'row insert skipped');
    }

}

sub rowupdateskipped {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'row update skipped');
    }

}

sub tabletransferdone {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'table transfer done: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub tablefixed {

    my ($target_db,$targettablename,$statement,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($target_db) . 'table fix applied to ' . $targettablename . ': ' . chopstring(trim($statement),90));
    }

}

sub tableprocessingdone {

    my ($db,$tablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info('table processing done: [' . $db->connectidentifier() . '].' . $tablename . ': ' . $numofrows . ' row(s)');
    }

}

sub rowtransferdone {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'row transfer done: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub fetching_rows {

    my ($db,$tablename,$start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'fetching rows from [' . $db->connectidentifier() . '].' . $tablename . ': ' . ($start + 1) . '-' . ($start + $blocksize) . ' of ' . $totalnumofrows);
    }

}

sub writing_rows {

    my ($db,$tablename,$start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getconnectorinstanceprefix($db) . 'writing rows to ' . $tablename . ': ' . ($start + 1) . '-' . ($start + $blocksize) . ' of ' . $totalnumofrows);
    }

}

sub processing_rows {

    my ($tid, $start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(($enablemultithreading ? '[' . $tid . '] ' : '') . 'processing rows: ' . ($start + 1) . '-' . ($start + $blocksize) . ' of ' . $totalnumofrows);
    }

}

sub mainconfigurationloaded {

    my ($configfile,$logger) = @_;
    if (defined $logger) {
        $logger->info('system configuration file ' . $configfile . ' loaded');
    }
    log_mainconfig(\&configinfo,$logger);

}

sub configinfo {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub cleanupinfo {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub servicedebug {

    my ($service, $message, $logger) = @_;
    if (defined $logger) {
        $message = '[' . $service->{tid} . '] ' . $service->identifier() . ' - ' . $message;
        $logger->debug($message);
    }

    #die();

}

sub serviceinfo {

    my ($service, $message, $logger) = @_;
    if (defined $logger) {
        $message = '[' . $service->{tid} . '] ' . $service->identifier() . ' - ' . $message;
        $logger->info($message);
    }

    #die();

}

sub _getconnectorinstanceprefix {
    my ($db) = @_;
    my $instancestring = $db->instanceidentifier();
    if (length($instancestring) > 0) {
    if ($db->{tid} != $root_threadid) {
        return '[' . $db->{tid} . '/' . $instancestring . '] ';
    } else {
        return '[' . $instancestring . '] ';
    }
    } elsif ($db->{tid} != $root_threadid) {
    return '[' . $db->{tid} . '] ';
    }
    return '';
}

sub _getconnectidentifiermessage {
    my ($db,$message) = @_;
    my $result = $db->connectidentifier();
    my $connectidentifier = $db->_connectidentifier();
    if (length($result) > 0 and defined $db->cluster and length($connectidentifier) > 0) {
    $result .= '->' . $connectidentifier;
    }
    if (length($result) > 0) {
    $result .= ' - ';
    }
    return $result . $message;
}

1;