package NGCP::BulkProcessor::LogError;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $system_version
    $erroremailrecipient
    $warnemailrecipient
    $doneemailrecipient
    $completionemailrecipient
    $appstartsecs
    $root_threadid
);

use NGCP::BulkProcessor::Mail qw(
    send_message
    send_email
    $signature
    wrap_mailbody
    $lowpriority
    $normalpriority
);
use NGCP::BulkProcessor::Utils qw(
    threadid
    create_guid
    getscriptpath
    timestamp
    secs_to_years
);

use POSIX qw(ceil); # locale_h);
#setlocale(LC_NUMERIC, 'C'); ->utils

use File::Basename qw(basename);

use Time::HiRes qw(time);

use Carp qw(carp cluck croak confess);
#$Carp::Verbose = 1;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    notimplementederror
    dberror
    dbwarn
    fieldnamesdiffer
    transferzerorowcount
    processzerorowcount
    deleterowserror

    tabletransferfailed
    tableprocessingfailed

    resterror
    restwarn
    restrequesterror
    restresponseerror

    fileerror
    filewarn

    processzerofilesize
    fileprocessingfailed
    fileprocessingerror
    fileprocessingwarn

    restprocessingfailed

    emailwarn
    configurationwarn
    configurationerror

    sortconfigerror

    xls2csverror
    xls2csvwarn

    serviceerror
    servicewarn

    webarchivexls2csverror
    webarchivexls2csvwarn

    dbclustererror
    dbclusterwarn

    done
    completion

    scripterror
    scriptwarn
);

my $erroremailsubject = 'error: module ';
my $warnemailsubject = 'warning: module ';
my $donemailsubject = 'done: module ';
my $completionmailsubject = 'completed: module ';

sub done {

    my ($message,$attachments,$logger) = @_;

    if (length($message) == 0) {
        $message = 'done';
    }

    my $appexitsecs = Time::HiRes::time();
    #$message .= "\n\n" . sprintf("%.2f",$appexitsecs - $appstartsecs) . ' seconds';
    $message .= "\n\n" . 'time elapsed: ' . secs_to_years(ceil($appexitsecs - $appstartsecs));

    if (defined $logger) {
        $logger->info($message);
    }

    if (threadid() == $root_threadid) {
        if (length($doneemailrecipient) > 0 and defined $logger) {
            my $email = {
                to          => $doneemailrecipient,
                #cc          => 'rkrenn@sipwise.com',
                #bcc         => '',
                #return_path => undef,
                priority    => $lowpriority,
                #sender_name => 'Rene K.',
                #from        => 'rkrenn@sipwise.com',
                subject     => $donemailsubject . $logger->{category},
                body        => getscriptpath() . ":\n\n" . wrap_mailbody($message) . "\n\n" . $signature,
                guid        => create_guid()
            };

            my ($mailresult,$mailresultmessage) = send_email($email,$attachments,\&fileerror,\&emailwarn);
        }

    }

}

sub completion {

    my ($message,$attachments,$logger) = @_;

    if (length($message) == 0) {
        $message = 'completed';
    }

    my $appexitsecs = Time::HiRes::time();
    #$message .= "\n\n" . sprintf("%.2f",$appexitsecs - $appstartsecs) . ' seconds';
    $message .= "\n\n" . 'time elapsed: ' . secs_to_years(ceil($appexitsecs - $appstartsecs));

    if (defined $logger) {
        $logger->info($message);
    }

    if (threadid() == $root_threadid) {
        if (length($completionemailrecipient) > 0 and defined $logger) {
            my $email = {
                to          => $completionemailrecipient,
                #cc          => 'rkrenn@sipwise.com',
                #bcc         => '',
                #return_path => undef,
                priority    => $normalpriority,
                #sender_name => 'Rene K.',
                #from        => 'rkrenn@sipwise.com',
                subject     => $completionmailsubject . $logger->{category},
                body        => getscriptpath() . ":\n\n" . wrap_mailbody($message) . "\n\n" . $signature,
                guid        => create_guid()
            };

            my ($mailresult,$mailresultmessage) = send_email($email,$attachments,\&fileerror,\&emailwarn);
        }

        #exit(0);
    }

}

sub warning {

    my ($message,$logger,$sendemail) = @_;

    if (threadid() == $root_threadid) {
        if ($sendemail and length($warnemailrecipient) > 0 and defined $logger) {
            my ($mailresult,$mailresultmessage) = send_message($warnemailrecipient,$warnemailsubject . $logger->{category},getscriptpath() . ":\n\n" . wrap_mailbody($message) . "\n\n" . $signature,\&fileerror,\&emailwarn);
        }
        carp($message);
        #warn($message);
    } else {
        carp($message);
        #warn($message);
    }

}

sub terminate {

    my ($message,$logger) = @_;

    if (threadid() == $root_threadid) {

        my $appexitsecs = Time::HiRes::time();
        #$message .= "\n\n" . sprintf("%.2f",$appexitsecs - $appstartsecs) . ' seconds';
        $message .= "\n\n" . 'time elapsed: ' . secs_to_years(ceil($appexitsecs - $appstartsecs));

        if (length($erroremailrecipient) > 0 and defined $logger) {
            my ($mailresult,$mailresultmessage) = send_message($erroremailrecipient,$erroremailsubject . $logger->{category},getscriptpath() . ":\n\n" . wrap_mailbody($message) . "\n\n" . $signature,\&fileerror,\&emailwarn);
        }
        croak($message); # confess...
        #die($message);
    } else {

        croak($message);
        #die($message);
    }

}

#sub registerthread {
#
#    my $thrlogger = shift;
#    $registered_tids{threads->tid()} = 1;
#    $SIG{'DIE'} = sub {
#
#                            print "signal\n";
#                            my $tid = threads->tid();
#                            my $message = '[' . $tid . '] aborting';
#                            if (defined $thrlogger) {
#                                $thrlogger->error('[' . $tid . '] aborting');
#                            }
#                            unregisterthread($tid);
#                            #threads->exit();
#                            croak($message);
#                        };
#
#}
#
#sub unregisterthread {
#
#    my $tid = shift;
#    if (!defined $tid) {
#        $tid = threads->tid();
#    }
#    delete $registered_tids{$tid};
#
#}

#sub terminatethreads {
#
#    # Loop through all the threads
#    foreach my $thr (threads->list()) {
#        # Don't join the main thread or ourselves
#        if ($thr->tid != 0 && !threads::equal($thr,threads->self)) {
#            $thr->kill('DIE'); #->detach();
#        }
#    }
#
#}

sub notimplementederror {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub dberror {

    my ($db, $message, $logger) = @_;
    $message = _getsqlconnectorinstanceprefix($db) . _getsqlconnectidentifiermessage($db,$message);
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub dbwarn {

    my ($db, $message, $logger) = @_;
    $message = _getsqlconnectorinstanceprefix($db) . _getsqlconnectidentifiermessage($db,$message);
    if (defined $logger) {
        $logger->warn($message);
    }

    #die();
    warning($message, $logger);

}


sub resterror {

    my ($restapi, $message, $logger) = @_;
    $message = _getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message);
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub restwarn {

    my ($restapi, $message, $logger) = @_;
    $message = _getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message);
    if (defined $logger) {
        $logger->warn($message);
    }

    #die();
    warning($message, $logger);

}

sub restrequesterror {

    my ($restapi, $message, $request, $logger) = @_;
    $message = _getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message);
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub restresponseerror {

    my ($restapi, $message, $response, $logger) = @_;
    $message = _getrestconnectorinstanceprefix($restapi) . _getrestconnectidentifiermessage($restapi,$message);
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub fieldnamesdiffer {

    my ($db,$tablename,$expectedfieldnames,$fieldnamesfound,$logger) = @_;
    my $message = _getsqlconnectorinstanceprefix($db) . 'wrong table fieldnames (v ' . $system_version . '): [' . $db->connectidentifier() . '].' . $tablename . ":\nexpected: " . ((defined $expectedfieldnames) ? join(', ',@$expectedfieldnames) : '<none>') . "\nfound:    " . ((defined $fieldnamesfound) ? join(', ',@$fieldnamesfound) : '<none>');
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub dbclustererror {

    my ($clustername,$message,$logger) = @_;
    $message = 'database cluster ' . $clustername . ': ' . $message;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);

}

sub dbclusterwarn {

    my ($clustername,$message,$logger) = @_;
    $message = 'database cluster ' . $clustername . ': ' . $message;
    if (defined $logger) {
        $logger->warn($message);
    }

    #die();
    warning($message, $logger);

}

sub transferzerorowcount {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    my $message = _getsqlconnectorinstanceprefix($db) . '[' . $db->connectidentifier() . '].' . $tablename . ' has 0 rows';
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub processzerorowcount {

    my ($db,$tablename,$numofrows,$logger) = @_;
    my $message = '[' . $db->connectidentifier() . '].' . $tablename . ' has 0 rows';
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub deleterowserror {

    my ($db,$tablename,$message,$logger) = @_;
    $message = _getsqlconnectorinstanceprefix($db) . '[' . $db->connectidentifier() . '].' . $tablename . ' - ' . $message;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub tabletransferfailed {

    my ($db,$tablename,$target_db,$targettablename,$numofrows,$logger) = @_;
    my $message = _getsqlconnectorinstanceprefix($db) . 'table transfer failed: [' . $db->connectidentifier() . '].' . $tablename . ' > ' . $targettablename;
    if (defined $logger) {
        $logger->error($message);
    }
    terminate($message, $logger);

}

sub tableprocessingfailed {

    my ($db,$tablename,$numofrows,$logger) = @_;
    my $message = 'table processing failed: [' . $db->connectidentifier() . '].' . $tablename;
    if (defined $logger) {
        $logger->error($message);
    }
    terminate($message, $logger);

}

sub fileerror {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

#sub yamlerror {
#
#    my ($message, $logger) = @_;
#    if (defined $logger) {
#        $logger->error($message);
#    }
#
#    terminate($message, $logger);
#    #terminatethreads();
#    #die();
#
#}

sub processzerofilesize {

    my ($file,$logger) = @_;
    my $message = basename($file) . ' ' . (-e $file ? 'has 0 bytes' : 'not found');
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub fileprocessingfailed {

    my ($file,$logger) = @_;
    my $message = 'file processing failed: ' . basename($file);
    if (defined $logger) {
        $logger->error($message);
    }
    terminate($message, $logger);

}

sub fileprocessingerror {

    my ($file,$message,$logger) = @_;
    my $message = basename($file) . ': ' . $message;
    if (defined $logger) {
        $logger->error($message);
    }
    terminate($message, $logger);

}

sub fileprocessingwarn {

    my ($file,$message,$logger) = @_;
    my $message = basename($file) . ': ' . $message;
    if (defined $logger) {
        $logger->warn($message);
    }
    warning($message, $logger);

}

sub restprocessingfailed {

    my ($restapi,$path_query,$logger) = @_;
    my $message = 'collection processing failed: [' . $restapi->connectidentifier() . '] ' . $path_query;
    if (defined $logger) {
        $logger->error($message);
    }
    terminate($message, $logger);

}

sub xls2csverror {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub webarchivexls2csverror {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub filewarn {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->warn($message);
    }

    #die();
    warning($message, $logger);
}


sub xls2csvwarn {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->warn($message);
    }

    warning($message, $logger);
}

sub webarchivexls2csvwarn {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->warn($message);
    }

    warning($message, $logger);
}

#sub parameterdefinedtwice {
#
#    my ($message,$logger) = @_;
#    if (defined $logger) {
#        $logger->warn($message);
#    }
#    warning($message, $logger);
#}

sub emailwarn {

    my ($message, $errormsg, $response, $logger) = @_;
    if (defined $logger) {
        if (length($response) > 0) {
            $logger->warn($message . ': ' . $errormsg . ' \'' . $response . '\'');
        } else {
            $logger->warn($message . ': ' . $errormsg);
        }
    }

    warning($message, $logger, 0);

}

sub configurationwarn {

    my ($configfile,$message,$logger) = @_;
    $message = 'configuration file ' . $configfile . ': ' . $message;
    if (defined $logger) {
        $logger->warn($message);
    }
    warning($message, $logger, 0);

}

sub configurationerror {

    my ($configfile,$message,$logger) = @_;
    $message = 'configuration file ' . $configfile . ': ' . $message;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);

}

sub sortconfigerror {

    my ($identifier,$message,$logger) = @_;

    if (defined $identifier) {
        $message = 'sort configuration (' . $identifier . '): ' . $message;
    } else {
        $message = 'sort configuration: ' . $message;
    }
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);

}

sub serviceerror {

    my ($service, $message, $logger) = @_;
    $message = '[' . $service->{tid} . '] ' . $service->identifier() . ' - ' . $message;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);

}

sub servicewarn {

    my ($service, $message, $logger) = @_;
    $message = '[' . $service->{tid} . '] ' . $service->identifier() . ' - ' . $message;
    if (defined $logger) {
        $logger->warn($message);
    }

    #die();
    warning($message, $logger);

}

sub _getsqlconnectorinstanceprefix {
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

sub scripterror {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->error($message);
    }

    terminate($message, $logger);
    #terminatethreads();
    #die();

}

sub scriptwarn {

    my ($message, $logger) = @_;
    if (defined $logger) {
        $logger->warn($message);
    }

    warning($message, $logger);

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

sub _getrestconnectorinstanceprefix {
    my ($restapi) = @_;
    my $instancestring = $restapi->instanceidentifier();
    if (length($instancestring) > 0) {
    if ($restapi->{tid} != $root_threadid) {
        return '[' . $restapi->{tid} . '/' . $instancestring . '] ';
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
    my $connectidentifier = $restapi->_connectidentifier();
    if (length($result) > 0 and length($connectidentifier) > 0) {
    $result .= '->' . $connectidentifier;
    }
    if (length($result) > 0) {
    $result .= ' - ';
    }
    return $result . $message;
}

1;
