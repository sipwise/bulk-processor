package NGCP::BulkProcessor::Logging;
use strict;

## no critic

use threads; # as early as possible...
use threads::shared;

use NGCP::BulkProcessor::Globals qw(
    $root_threadid
    $logfile_path
    $fileloglevel
    $emailloglevel
    $screenloglevel
    $enablemultithreading
);

use Log::Log4perl qw(get_logger);

*Log::Log4perl::Logger::notice = *Log::Log4perl::Logger::info;
*Log::Log4perl::Logger::warning = *Log::Log4perl::Logger::warn;

use File::Basename qw(basename);

use NGCP::BulkProcessor::Utils qw(timestampdigits datestampdigits changemod chopstring trim humanize_bytes);
use NGCP::BulkProcessor::Array qw (contains);

require Exporter;
our @ISA = qw(Exporter);

our @EXPORT_OK = qw(
    getlogger

    cleanuplogfiles

    emailinfo
    emaildebug
    dbdebug
    dbinfo
    restdebug
    restinfo
    nosqldebug
    nosqlinfo

	attachmentdownloaderdebug
    attachmentdownloaderinfo

    fieldnamesaquired
    fieldnamesacquired
    primarykeycolsaquired
    primarykeycolsacquired
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

    rowinserted
    rowupserted
    rowsupserted
    rowupdated
    rowsdeleted
    rowsupdated
    totalrowsdeleted
    rowinsertskipped
    rowupdateskipped
    tabletransferdone
    tableprocessingdone
    rowtransferdone
    fetching_rows
    writing_rows
    processing_rows


    configurationinfo
    init_log
    $currentlogfile
    $attachmentlogfile
    scriptinfo

    xls2csvinfo
    tablethreadingdebug

    filethreadingdebug
    fileprocessingstarted
    fileprocessingdone
    lines_read
    processing_lines

    processing_info
    processing_debug

    faketimeinfo
    faketimedebug

    restthreadingdebug
    restprocessingstarted
    restprocessingdone
    fetching_items
    processing_items

    nosqlthreadingdebug
    nosqlprocessingstarted
    nosqlprocessingdone
    fetching_entries
    processing_entries
    
    tablefixed
    servicedebug
    serviceinfo

    enable_threading_info
);
#rowskipped

my $logfileextension = '.log';

our $currentlogfile;
#our $weblogfile;
our $attachmentlogfile;

my $loginitialized = 0;

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
               "log4perl.appender.ScreenApp.layout    = Log::Log4perl::Layout::PatternLayout\n" .
               'log4perl.appender.ScreenApp.layout.ConversionPattern = %m%n';

    # Initialize logging behaviour
    Log::Log4perl->init( \$conf );
    get_logger(__PACKAGE__)->debug('default log4perl configuration applied');

}

sub init_log {

    my ($ts,$name,$daemon_logfile) = @_;
 
    $name //= '';
    $name = '_' . $name if $name;

    undef $currentlogfile;
    undef $attachmentlogfile;

    if ($daemon_logfile) {
        $currentlogfile = sprintf($daemon_logfile,$name);
    } else {
        $ts //= time;
        $ts = timestampdigits($ts);
        $currentlogfile = $logfile_path . $ts . $name .  $logfileextension;
        $attachmentlogfile = $logfile_path . 'email_' . $ts . $name . $logfileextension;
    }

    # log configuration
    my @loggers = ( 'DEBUG' );
    my $conf = '';
    if (length($fileloglevel) and 'off' ne lc($fileloglevel) and $currentlogfile) {
        createlogfile($currentlogfile);
        $conf .= "log4perl.appender.FileApp             = Log::Log4perl::Appender::File\n" .
               "log4perl.appender.FileApp.umask       = 0\n" .
               "log4perl.appender.FileApp.syswite     = 1\n" .
               'log4perl.appender.FileApp.Threshold   = ' . $fileloglevel . "\n" .
               "log4perl.appender.FileApp.mode        = append\n" .
               'log4perl.appender.FileApp.filename    = ' . $currentlogfile . "\n" .
               "log4perl.appender.FileApp.create_at_logtime = 1\n" .
               "log4perl.appender.FileApp.layout      = PatternLayout\n" .
               'log4perl.appender.FileApp.layout.ConversionPattern = %d> %m%n' . "\n\n";
        push(@loggers,'FileApp');
    }
    if (length($emailloglevel) and 'off' ne lc($emailloglevel) and $attachmentlogfile) {
        createlogfile($attachmentlogfile);
        $conf .= "log4perl.appender.MailAttApp             = Log::Log4perl::Appender::File\n" .
               "log4perl.appender.MailApp.umask          = 0\n" .
               "log4perl.appender.MailApp.syswite        = 1\n" .
               'log4perl.appender.MailAttApp.Threshold   = ' . $emailloglevel . "\n" .
               "log4perl.appender.MailAttApp.mode        = append\n" .
               'log4perl.appender.MailAttApp.filename    = ' . $attachmentlogfile . "\n" .
               "log4perl.appender.MailAttApp.create_at_logtime = 1\n" .
               "log4perl.appender.MailAttApp.layout      = Log::Log4perl::Layout::SimpleLayout\n" .
               'log4perl.appender.MailAttApp.layout.ConversionPattern = %d> %m%n' . "\n\n";
        push(@loggers,'MailAttApp');
    }

    if (length($screenloglevel) and 'off' ne lc($screenloglevel)) {
        $conf .= "log4perl.appender.ScreenApp           = Log::Log4perl::Appender::Screen\n" .
               #"log4perl.appender.ScreenApp           = Log::Log4perl::Appender::ScreenColoredLevels\n" .
               'log4perl.appender.ScreenApp.Threshold = ' . $screenloglevel . "\n" .
               "log4perl.appender.ScreenApp.stderr    = 0\n" .
               "log4perl.appender.ScreenApp.layout    = Log::Log4perl::Layout::SimpleLayout\n" .
               'log4perl.appender.ScreenApp.layout.ConversionPattern = %d> %m%n';
        push(@loggers,'ScreenApp');
    }

    $conf = "log4perl.logger                       = " . join(',',@loggers) . "\n" . $conf;

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
    return eval { get_logger($package) };
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
        $logger->debug(_getsqlconnectorinstanceprefix($db) . _getsqlconnectidentifiermessage($db,$message));
    }

    #die();

}

sub dbinfo {

    my ($db, $message, $logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . _getsqlconnectidentifiermessage($db,$message));
    }

    #die();

}

sub nosqldebug {

    my ($connector, $message, $logger) = @_;
    if (defined $logger) {
        $logger->debug(_getnosqlconnectorinstanceprefix($connector) . _getnosqlconnectidentifiermessage($connector,$message));
    }

    #die();

}

sub nosqlinfo {

    my ($connector, $message, $logger) = @_;
    if (defined $logger) {
        $logger->info(_getnosqlconnectorinstanceprefix($connector) . _getnosqlconnectidentifiermessage($connector,$message));
    }

    #die();

}

sub restdebug {

    my ($restapi, $message, $logger) = @_;
    if (defined $logger) {
        $logger->debug(_getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message));
    }

    #die();

}

sub restinfo {

    my ($restapi, $message, $logger) = @_;
    if (defined $logger) {
        $logger->info(_getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message));
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

# Backwards compatibility alias.
*fieldnamesaquired = \&fieldnamesacquired;

sub fieldnamesacquired {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'fieldnames acquired and OK: [' . $db->connectidentifier() . '].' . $tablename);
    }

}

# Backwards compatibility alias.
*primarykeycolsaquired = \&primarykeycolsacquired;

sub primarykeycolsacquired {

    my ($db,$tablename,$keycols,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'primary key columns acquired for [' . $db->connectidentifier() . '].' . $tablename . ': ' . ((defined $keycols and scalar @$keycols > 0) ? join(', ',@$keycols) : '<no primary key columns>'));
    }

}

sub tableinfoscleared {

    my ($db,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'table infos cleared for ' . $db->connectidentifier());
    }

}

sub tabletransferstarted {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'table transfer started: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . (defined $numofrows ? ': ' . $numofrows . ' row(s)' : ''));
    }

}

sub tableprocessingstarted {

    my ($db,$tablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info('table processing started: [' . $db->connectidentifier() . '].' . $tablename . (defined $numofrows ? ': ' . $numofrows . ' row(s)' : ''));
    }

}

my $threading_info : shared = 0;

sub enable_threading_info {
    my $info = shift;
    lock $threading_info;
    $threading_info = 1;
}

sub tablethreadingdebug {

    my ($message,$logger) = @_;
    if (defined $logger) {
        lock $threading_info;
        if ($threading_info) {
            $logger->info($message);
        } else {    
            $logger->debug($message);
        }
    }

}

sub rowtransferstarted {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'row transfer started: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub texttablecreated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'text table created: ' . $tablename);
    }

}

sub indexcreated {

    my ($db,$tablename,$indexname,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'index created: ' . $indexname . ' on ' . $tablename);
    }

}

sub primarykeycreated {

    my ($db,$tablename,$keycols,$logger) = @_;
    if (defined $logger and (defined $keycols and scalar @$keycols > 0)) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'primary key created: ' . join(', ',@$keycols) . ' on ' . $tablename);
    }

}

sub temptablecreated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'temporary table created: ' . $tablename);
    }

}

sub tabletruncated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'table truncated: ' . $tablename);
    }

}

sub tabledropped {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'table dropped: ' . $tablename);
    }

}

sub rowtransferred {

    my ($db,$tablename,$target_db,$targettablename,$i,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row ' . $i . '/' . $numofrows . ' transferred');
    }

}

#sub rowskipped {
#
#    my ($db,$tablename,$target_db,$targettablename,$i,$numofrows,$logger) = @_;
#    if (defined $logger) {
#        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row ' . $i . '/' . $numofrows . ' skipped');
#    }
#
#}

sub rowinserted {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row inserted');
    }

}

sub rowupserted {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row upserted');
    }

}

sub rowsupserted {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row(s) upserted');
    }

}

sub rowupdated {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row updated');
    }

}

sub rowsupdated {

    my ($db,$tablename,$rowcount,$logger) = @_;
    if (defined $logger) {

        $logger->debug(_getsqlconnectorinstanceprefix($db) . $rowcount . ' row(s) updated');

    }

}

sub rowsdeleted {

    my ($db,$tablename,$rowcount,$initial_rowcount,$logger) = @_;
    if (defined $logger) {
    if (defined $initial_rowcount) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . $rowcount . ' of ' . $initial_rowcount . ' row(s) deleted');
    } else {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . $rowcount . ' row(s) deleted');
    }
    }

}

sub totalrowsdeleted {

    my ($db,$tablename,$rowcount_total,$initial_rowcount,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . $rowcount_total . ' of ' . $initial_rowcount . ' row(s) deleted from [' . $db->connectidentifier() . '].' . $tablename);
    }

}

sub rowinsertskipped {

    my ($db,$tablename,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . 'row insert skipped');
    }

}

sub rowupdateskipped {

    my ($db,$tablename,$matched,$logger) = @_;
    if (defined $logger) {
        $logger->debug(_getsqlconnectorinstanceprefix($db) . "row update skipped, $matched matching rows");
    }

}

sub tabletransferdone {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'table transfer done: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . (defined $numofrows ? ': ' . $numofrows . ' row(s)' : ''));
    }

}

sub tablefixed {

    my ($target_db,$targettablename,$statement,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($target_db) . 'table fix applied to ' . $targettablename . ': ' . chopstring(trim($statement),90));
    }

}

sub tableprocessingdone {

    my ($db,$tablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info('table processing done: [' . $db->connectidentifier() . '].' . $tablename . (defined $numofrows ? ': ' . $numofrows . ' row(s)' : ''));
    }

}

sub rowtransferdone {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'row transfer done: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename . ': ' . $numofrows . ' row(s)');
    }

}

sub fetching_rows {

    my ($db,$tablename,$start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'fetching rows from [' . $db->connectidentifier() . '].' . $tablename . ': ' . ($start + 1) . '-' . ($start + $blocksize) . (defined $totalnumofrows ? ' of ' . $totalnumofrows : ''));
    }

}

sub writing_rows {

    my ($db,$tablename,$start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getsqlconnectorinstanceprefix($db) . 'writing rows to ' . $tablename . ': ' . ($start + 1) . '-' . ($start + $blocksize) . (defined $totalnumofrows ? ' of ' . $totalnumofrows : ''));
    }

}

sub processing_rows {

    my ($context, $start,$blocksize,$totalnumofrows,$logger) = @_;
    if (defined $logger) {
        $logger->info(_processing_prefix($context) . 'processing rows: ' . ($start + 1) . '-' . ($start + $blocksize) . (defined $totalnumofrows ? ' of ' . $totalnumofrows : ''));
    }

}

sub _processing_prefix {
    my $context = shift;
    $context = { tid => $context, } unless ref $context;
    my $name = '';
    $name = $context->{name} if exists $context->{name};
    if (length($name) > 0) {
    if ($context->{tid} != $root_threadid) {
        return '[' . $context->{tid} . ' ' . $name . '] ';
    } else {
        return '[' . $name . '] ';
    }
    } elsif ($context->{tid} != $root_threadid) {
    return '[' . $context->{tid} . '] ';
    }
    return '';
}


sub filethreadingdebug {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}

sub fileprocessingstarted {

    my ($file,$logger) = @_;
    if (defined $logger) {
        $logger->info('file processing started: ' . basename($file) . ' (' . humanize_bytes(-s $file) . ')');
    }

}

sub fileprocessingdone {

    my ($file,$logger) = @_;
    if (defined $logger) {
        $logger->info('file processing done: ' . basename($file));
    }

}

#sub fetching_lines {
#
#    my ($file,$start,$blocksize,$block_n,$logger) = @_;
#    if (defined $logger) {
#        if (defined $block_n) {
#            $logger->info('fetching lines from ' . basename($file) . ': ' . humanize_bytes($block_n));
#        } else {
#            $logger->info('fetching lines from ' . basename($file) . ': ' . ($start + 1) . '~' . ($start + $blocksize));
#        }
#    }
#
#}


sub lines_read {

    my ($file,$start,$blocksize,$block_n,$logger) = @_;
    if (defined $logger) {
        if (defined $block_n) {
            if ($block_n > 0) {
                $logger->info(basename($file) . ': ' . humanize_bytes($block_n) . ' read');
            }
        } else {
            $logger->info(basename($file) . ': lines ' . ($start + 1) . '~' . ($start + $blocksize) . ' read');
        }
    }

}

sub processing_lines {

    my ($context, $start,$blocksize,$block_n,$logger) = @_;
    if (defined $logger) {
        if (defined $block_n) {
            if ($block_n > 0) {
                $logger->info(_processing_prefix($context) . 'processing lines: ' . humanize_bytes($block_n));
            }
        } else {
            $logger->info(_processing_prefix($context) . 'processing lines: ' . ($start + 1) . '-' . ($start + $blocksize));
        }
    }

}

sub processing_info {

    my ($context, $message, $logger) = @_;
    if (defined $logger) {
        $logger->info(_processing_prefix($context) . $message);
    }

}

sub processing_debug {

    my ($context, $message, $logger) = @_;
    if (defined $logger) {
        $logger->debug(_processing_prefix($context) . $message);
    }

}


sub restthreadingdebug {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}

sub fetching_items {

    my ($restapi,$path_query,$start,$blocksize,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,'fetching ' . $path_query . ' collection page: ' . ($start + 1) . '-' . ($start + $blocksize)));
    }

}

sub processing_items {

    my ($context, $start,$blocksize,$logger) = @_;
    if (defined $logger) {
        $logger->info(_processing_prefix($context) . 'processing items: ' . ($start + 1) . '-' . ($start + $blocksize));
    }

}

sub restprocessingstarted {

    my ($restapi,$path_query,$logger) = @_;
    if (defined $logger) {
        $logger->info('collection processing started: [' . $restapi->connectidentifier() . '] ' . $path_query);
    }

}

sub restprocessingdone {

    my ($restapi,$path_query,$logger) = @_;
    if (defined $logger) {
        $logger->info('collection processing done: [' . $restapi->connectidentifier() . '] ' . $path_query);
    }

}


sub nosqlthreadingdebug {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}

sub fetching_entries {

    my ($store,$scan_pattern,$start,$blocksize,$logger) = @_;
    if (defined $logger) {
        $logger->info(_getnosqlconnectorinstanceprefix($store) . _getnosqlconnectidentifiermessage($store,'fetching ' . ($scan_pattern ? "$scan_pattern " : '') . 'entries: ' . ($start + 1) . '-' . ($start + $blocksize)));
    }

}

sub processing_entries {

    my ($context, $start, $blocksize, $logger) = @_;
    if (defined $logger) {
        if ($blocksize) {
            $logger->info(_processing_prefix($context) . 'processing entries: ' . ($start + 1) . '-' . ($start + $blocksize));
        } else {
            $logger->info(_processing_prefix($context) . 'processing entries: (none)');
        }
    }

}

sub nosqlprocessingstarted {

    my ($store,$scan_pattern,$logger) = @_;
    if (defined $logger) {
        my $msg = 'keystore processing started: ';
        my $connectidentifier = $store->connectidentifier();
        if ($connectidentifier) {
            $msg .= '[' . $connectidentifier . '] ';
        }
        $msg .= $scan_pattern;
        
        $logger->info($msg);
    }

}

sub nosqlprocessingdone {

    my ($store,$scan_pattern,$logger) = @_;
    if (defined $logger) {
        my $msg = 'keystore processing done: ';
        my $connectidentifier = $store->connectidentifier();
        if ($connectidentifier) {
            $msg .= '[' . $connectidentifier . '] ';
        }
        $msg .= $scan_pattern;
        
        $logger->info($msg);
    }

}

#sub mainconfigurationloaded {
#
#    my ($configfile,$logger) = @_;
#    if (defined $logger) {
#        $logger->info('system configuration file ' . $configfile . ' loaded');
#    }
#    log_mainconfig(\&configinfo,$logger);
#
#}

sub configurationinfo {

    my ($message,$logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub faketimeinfo {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->info($message);
    }

}

sub faketimedebug {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->debug($message);
    }

}


sub scriptinfo {

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

sub _getsqlconnectorinstanceprefix {
    my ($db) = @_;
    my $instancestring = $db->instanceidentifier();
    if (length($instancestring) > 0) {
    if ($db->{tid} != $root_threadid) {
        return '[' . $db->{tid} . ' ' . $instancestring . '] ';
    } else {
        return '[' . $instancestring . '] ';
    }
    } elsif ($db->{tid} != $root_threadid) {
    return '[' . $db->{tid} . '] ';
    }
    return '';
}

sub _getsqlconnectidentifiermessage {
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

sub _getnosqlconnectorinstanceprefix {
    my ($connector) = @_;
    my $instancestring = $connector->instanceidentifier();
    if (length($instancestring) > 0) {
    if ($connector->{tid} != $root_threadid) {
        return '[' . $connector->{tid} . ' ' . $instancestring . '] ';
    } else {
        return '[' . $instancestring . '] ';
    }
    } elsif ($connector->{tid} != $root_threadid) {
    return '[' . $connector->{tid} . '] ';
    }
    return '';
}

sub _getnosqlconnectidentifiermessage {
    my ($connector,$message) = @_;
    my $result = $connector->connectidentifier();
    if (length($result) > 0) {
    $result .= ' - ';
    }
    return $result . $message;
}

sub _getrestconnectorinstanceprefix {
    my ($restapi) = @_;
    my $instancestring = $restapi->instanceidentifier();
    if (length($instancestring) > 0) {
    if ($restapi->{tid} != $root_threadid) {
        return '[' . $restapi->{tid} . ' ' . $instancestring . '] ';
    } else {
        return '[' . $instancestring . '] ';
    }
    } elsif ($restapi->{tid} != $root_threadid) {
    return '[' . $restapi->{tid} . '] ';
    }
    return '';
}

sub _getrestconnectidentifiermessage {
    my ($restapi,$message) = @_;
    my $result = $restapi->connectidentifier();
    if (length($result) > 0) {
    $result .= ' - ';
    }
    return $result . $message;
}

1;
