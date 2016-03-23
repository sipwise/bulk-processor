# mail module: sending emails with attachments

package Mail;
#BEGIN { $INC{Mail} ||= __FILE__ };
use strict;

#require Logging;
use Logging qw(
getlogger
emailinfo
emaildebug);
#use LogError qw(fileerror);
#use LogWarn qw(emailwarn);

#use LoadConfig;
use Globals qw(
    $system_name
    $system_instance_label
    $system_version
    $local_fqdn
    $mailfilepath
    $emailenable
    $mailprog
    $mailtype

    $ismsexchangeserver
    $sender_address
    $smtp_server
    $smtpuser
    $smtppasswd
    $writefiles
);

use Utils qw(trim file_md5 create_guid wrap_text changemod);

use File::Basename;
#use File::Temp qw(tempfile tempdir);
use MIME::Base64;
use MIME::Lite;

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    send_message
    send_email
    wrap_mailbody
    $signature
    $normalpriority
    $lowpriority
    $highpriority
    cleanupmsgfiles
);

my $wordwrapcolumns = 72; #linebreak/wrap columns

our $signature = "--\n" . $system_name . ' ' . $system_version . ' (' . $system_instance_label . ")\n[" . $local_fqdn . ']'; # a nice email signature

my $msgextension = '.msg';

our $normalpriority = 0;
our $lowpriority = 1;
our $highpriority = 2;

my %msmailpriority = (
  $normalpriority => 'Normal',
  $lowpriority    => 'Low',
  $highpriority   => 'High');

# sample email data structure:
#my $message = {
#    to          => 'rkrenn@sipwise.com',
#    cc          => 'rkrenn@alumni.tugraz.at',
#    bcc         => '',
#    return_path => '',
#    priority    => $normalpriority,
#    sender_name => 'Rene Krenn',
#    from        => 'rkrenn@sipwise.com',
#    subject     => 'subject...',
#    body        => wrap_mailbody('test.......'),
#    guid        => create_guid()
#};


my $mailsentsuccessfully = 1;

my $mailingdisabled = 0;

my $smtpserveripinvalid = -11;
my $smtpsocketcreatefailed = -12;
my $smtpserverconnectfailed = -13;
my $smtpprotocolerrorinitial = -14;
my $smtpprotocolerrorehlo = -15; #list auth options - esmtp
my $smtpnoauthloginavailable = -16;
my $smtpprotocolerrorlogin = -17; #start login
my $smtpprotocolerroruser = -18;
my $smtpprotocolerrorpass = -19;
my $smtpprotocolerrorhelo = -20; #normal smtp - no auth
my $mailrecipientundefined = -21;
my $smtpprotocolerrorfrom = -20;
my $smtpprotocolerrorrcpt = -22;
my $smtpprotocolerrordata = -23;


my $smtpnetfatalerror = -24;
my $smtpprotocolerrordataaccepted = -25;


my $mailprogpipeerror = -26;
my $writemailfileerror = -27;

my $errorcreatingmimemail = -30;
my $errorcreatingfileattachment = -32;
my $attachmentfilenotexistent = -31;



my $mailerr_messages = {
    $mailsentsuccessfully => 'MailSentSuccessfully',
    $mailingdisabled => 'MailingDisabled',

    $smtpserveripinvalid => 'SMTPServerIPInvalid',
    $smtpsocketcreatefailed => 'SMTPSocketCreateFailed',
    $smtpserverconnectfailed => 'SMTPServerConnectFailed',
    $smtpprotocolerrorinitial => 'SMTPProtocolErrorInitial',
    $smtpprotocolerrorehlo => 'SMTPProtocolErrorEhlo',
    $smtpnoauthloginavailable => 'SMTPNoAuthLoginAvailable',
    $smtpprotocolerrorlogin => 'SMTPProtocolErrorLogin',
    $smtpprotocolerroruser => 'SMTPProtocolErrorUser',
    $smtpprotocolerrorpass => 'SMTPProtocolErrorPass',
    $smtpprotocolerrorhelo => 'SMTPProtocolErrorHelo',
    $smtpprotocolerrorfrom => 'SMTPProtocolErrorFrom',
    $smtpprotocolerrorrcpt => 'SMTPProtocolErrorRcpt',
    $smtpprotocolerrordata => 'SMTPProtocolErrorData',

    $smtpnetfatalerror => 'SMTPNetFatalError',
    $smtpprotocolerrordataaccepted => 'SMTPProtocolErrorDataAccepted',

    $mailprogpipeerror => 'MailProgPipeError',

    $writemailfileerror => 'WriteMailFileError',

    $mailrecipientundefined  => 'MailRecipientUndefined',
    $errorcreatingmimemail => 'ErrorCreatingMIMEMail',
    $errorcreatingfileattachment => 'ErrorCreatingFileAttachment',

    $attachmentfilenotexistent => 'AttachmentFileNotExistent',

};

my $logger = getlogger(__PACKAGE__);

# email body wordwrapping:
sub wrap_mailbody {

    return wrap_text(shift,$wordwrapcolumns);

}

# send an email
# $mailmessage parameter is a email data structure
# $filepaths is an arrayref with filenames to attach
sub send_mail_with_attachments {

    my ($mailmessage,$filepaths,$fileerrorcode, $emailwarncode) = @_;

    my @filestocleanup = ();

    my $message = $mailmessage->{body};
    $message =~ s/^\./\.\./gm;
    $message =~ s/\r\n/\n/g;

    my $to = cleanrcpts($mailmessage->{to});
    my $cc = cleanrcpts($mailmessage->{cc});
    my $bcc = cleanrcpts($mailmessage->{bcc});
    my $returnpath = preparemailaddress($mailmessage->{return_path});
    my $priority = $msmailpriority{$mailmessage->{priority}};

    my $mime_mail = MIME::Lite->new(
        From              => '"' . $mailmessage->{sender_name} . '" <' . preparemailaddress($mailmessage->{from}) . '>',
        Sender            => $system_name,
        Type              => 'multipart/mixed',
        Encoding          => 'binary',
        Subject           => $mailmessage->{subject}
    ) or return $errorcreatingmimemail;

    $mime_mail->add('Message-ID' => '<' . $mailmessage->{guid} . '@' . $local_fqdn . '>');
    $mime_mail->add('X-Mailer' => $system_name . ' Plaintext Mailer');

    if (defined $to and $to ne '') {
      $mime_mail->add('To' => $to);
    }
    if (defined $cc and $cc ne '') {
      $mime_mail->add('Cc' => $cc);
    }
    if (defined $bcc and $bcc ne '') {
      $mime_mail->add('Bcc' => $bcc);
    }
    if (defined $returnpath and $returnpath ne '') {
      $mime_mail->add('Return-Path' => '<' . $returnpath . '>');
    }
    if (defined $priority and $priority ne '') {
      $mime_mail->add('Importance' => $priority);
    }

    $mime_mail->attr('content-type.charset' => 'UTF8');

    $mime_mail->attach(
      Type     => 'TEXT',
      Data     => $message
    );

    if (defined $filepaths and ref $filepaths eq 'ARRAY') {

      my @attachmentfilepaths = @$filepaths;

      for (my $i = 0; $i < scalar @attachmentfilepaths; $i++) {
          my $attachmentfilepath = $attachmentfilepaths[$i];
          if (-e $attachmentfilepath) {
              my $filesize = -s $attachmentfilepath;
              #push @filestocleanup,$attachmentfilepath;
              if ($filesize > 0) {
                  $mime_mail->attach(
                      Id          => file_md5($attachmentfilepath,$fileerrorcode,$logger),
                      Type        => 'AUTO',
                      Filename    => basename($attachmentfilepath),
                      Length      => $filesize,
                      Encoding    => 'base64',
                      Disposition => 'attachment',
                      ReadNow     => 1,
                      Path        => $attachmentfilepath
                  ) or return flushtempfiles($errorcreatingfileattachment,\@filestocleanup);
              }
          } else {
              return flushtempfiles($attachmentfilenotexistent,\@filestocleanup);
          }
      }

    }

    return flushtempfiles(send_smtp(preparemailaddress($mailmessage->{from}), mergercpts(($mailmessage->{to},$mailmessage->{cc},$mailmessage->{bcc})), $mime_mail->as_string(),$fileerrorcode, $emailwarncode),\@filestocleanup);

}

sub flushtempfiles {

    my ($errorcode,$filestocleanup) = @_;
    foreach my $filetocleanup (@$filestocleanup) {
    unlink $filetocleanup;
    }
    return $errorcode;

}

sub send_simple_mail {

    my ($to, $subject, $messagebody, $from, $from_name, $return_path,$fileerrorcode, $emailwarncode) = @_;

    my $message = $messagebody;
    $message =~ s/^\./\.\./gm;
    $message =~ s/\r\n/\n/g;
    $message =~ s/<\/*b>//g;

    my $crlf = "\n";
    if ($ismsexchangeserver) {
    $crlf = "\r\n";
    }

    my $fromemail = preparemailaddress($from);
    my $returnpath = preparemailaddress($return_path);

    my $data = 'From: ';
    if (defined $from_name and $from_name ne '') {
    $data .= '"' . $from_name . '" ';
    }
    if (defined $fromemail and $fromemail ne '') {
    $data .= '<' . $fromemail . '>' . $crlf;
    } else {
    $data .= '<' . $sender_address . '>' . $crlf;
    }
    $data .= 'Subject: ' . $subject . $crlf;
    $data .= 'To: ' . cleanrcpts($to) . $crlf;
    $data .= 'X-Mailer: ' . $system_name . ' Plaintext Mailer' . $crlf;
    if (defined $returnpath and $returnpath ne '') {
    $data .= 'Return-Path: <' . $returnpath . '>' . $crlf;
    }
    $data .= $message;

    return send_smtp($from, $to, $data,$fileerrorcode, $emailwarncode);

}

sub send_smtp {

    my ($from, $to, $data, $fileerrorcode, $emailwarncode) = @_;

    my $fromemail = preparemailaddress($from);

    if (!$to) {
    return $mailrecipientundefined;
    }

    my $crlf = "\n";
    if ($ismsexchangeserver) {
    $crlf = "\r\n";
    }

    local *MAIL;

    if ($mailtype == 1) {

    use Socket;

    my($proto) = (getprotobyname('tcp'))[2];
    my($port) = (getservbyname('smtp', 'tcp'))[2];
    my($smtpaddr) = ($smtp_server =~ /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/) ? pack('C4',$1,$2,$3,$4) : (gethostbyname($smtp_server))[4];

    if (!defined($smtpaddr)) { return $smtpserveripinvalid; }
    if (!socket(MAIL, AF_INET, SOCK_STREAM, $proto)) { return $smtpsocketcreatefailed; }
    if (!connect(MAIL, pack('Sna4x8', AF_INET, $port, $smtpaddr))) { return $smtpserverconnectfailed; }

    my($oldfh) = select(MAIL);
    $| = 1;
    select($oldfh);

    $_ = <MAIL>;
    if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorinitial;
    }

    if (defined $smtpuser and $smtpuser ne '') {

        #use MIME::Base64;
        print MAIL 'ehlo ' . $smtp_server . "\r\n";

        my $authloginavailable = 0;
        while (<MAIL>) {
        if (/^[45]/) {
            close(MAIL);
            return $smtpprotocolerrorehlo;
        } elsif (/auth.login/gi) { #auth login available
            $authloginavailable = ($authloginavailable or 1);
        } elsif (/ok/gi) {
            last;
        }
        }
        if ($authloginavailable == 0) {
        close(MAIL);
        return $smtpnoauthloginavailable;
        }

        print MAIL "auth login\r\n";
        $_ = <MAIL>;
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorlogin;
        }

        print MAIL encode_base64($smtpuser,'') . "\r\n";
        $_ = <MAIL>;
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerroruser;
        }

        print MAIL encode_base64($smtppasswd,'') . "\r\n";
        $_ = <MAIL>;
        #emaildebug($_,$logger);
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorpass; #auth unsuccessful
        }

    } else {

        print MAIL 'helo ' . $smtp_server . "\r\n";
        $_ = <MAIL>;
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorhelo;
        }

    }

    print MAIL 'mail from: <' . $fromemail . ">\r\n";
    $_ = <MAIL>;
    if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorfrom;
    }

    foreach (splitrcpts($to)) {
        print MAIL 'rcpt to: <' . $_ . ">\r\n";
        $_ = <MAIL>;
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrorrcpt;
        }
    }

    print MAIL "data\r\n";
    $_ = <MAIL>;
    if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrordata;
    }
    #print MAIL "123";
    #print MAIL $crlf . '.' . $crlf;
    #$_ = <MAIL>;

    } elsif ($mailtype == 0) {
    if (not open(MAIL,"| $mailprog -t")) {
        emailwarn('problem with pipe to ' . $mailprog . ': ' . $!,$logger);
        return $mailprogpipeerror;
    }
    }

    if ($mailtype == 2) {
    eval {
        use Net::SMTP;
        my $smtp;
        if (not $smtp = Net::SMTP->new($smtp_server, Debug => 0)) {
            #emailwarn('unable to create Net::SMTP object - ' . $smtp_server);
        #if (defined $emailwarncode and ref $emailwarncode eq 'CODE') {
        #  &$emailwarncode('unable to create Net::SMTP object - ' . $smtp_server);
        #}
        die('unable to create Net::SMTP object - ' . $smtp_server);
        } else {
        $smtp->mail($fromemail);
        if (defined $smtpuser and $smtpuser ne '') {
            $smtp->auth($smtpuser,$smtppasswd);
        }
        $smtp->to($to);
        $smtp->data();
        $smtp->datasend($data);
        $smtp->dataend();
        $smtp->quit();
        }
    };
    if ($@) {
        #emailwarn('Net::SMTP fatal error: ' . $@);
        if (defined $emailwarncode and ref $emailwarncode eq 'CODE') {
          &$emailwarncode('Net::SMTP fatal error: ' . $@);
        }
        return $smtpnetfatalerror;
    }
    return 1;
    } else {

    print MAIL $data;
    print MAIL $crlf . '.' . $crlf;
    #print MAIL "\n.\n"; #$crlf . '.' . $crlf;

    if ($mailtype == 1) {
        $_ = <MAIL>;
        emaildebug($_,$logger);
        if (/^[45]/) {
        close(MAIL);
        return $smtpprotocolerrordataaccepted;
        }
    }

    print MAIL "quit\r\n";

    if ($mailtype == 1) {
        $_ = <MAIL>;
    }

    close(MAIL);

    }

    if ($writefiles) {
    foreach my $rcpt (splitrcpts($to)) {
        my $fileindex = 0;
        my $emailfile = $mailfilepath . $rcpt . '.' . $fileindex . $msgextension;
        while (-e $emailfile) {
        $fileindex += 1;
        $emailfile = $mailfilepath . $rcpt . '.' . $fileindex . $msgextension;
        }
            local *MAILFILE;
        if (not open (MAILFILE,'>' . $emailfile)) {
        #fileerror('cannot open file ' . $emailfile . ': ' . $!,$logger);
        if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
          &$fileerrorcode('cannot open file ' . $emailfile . ': ' . $!,$logger);
        }
        return $writemailfileerror;
        }
        print MAILFILE $data;
        close(MAILFILE);
        changemod($emailfile);
    }
    }

    return $mailsentsuccessfully ;

}

sub cleanupmsgfiles {

    my ($fileerrorcode,$filewarncode) = @_;
    my $rmsgextension = quotemeta($msgextension);
    local *MAILDIR;
    if (not opendir(MAILDIR, $mailfilepath)) {
    #fileerror('cannot opendir ' . $mailfilepath . ': ' . $!,$logger);
    if (defined $fileerrorcode and ref $fileerrorcode eq 'CODE') {
        &$fileerrorcode('cannot opendir ' . $mailfilepath . ': ' . $!,$logger);
    }
    return;
    }
    my @files = grep { /$rmsgextension$/ && -f $mailfilepath . $_ } readdir(MAILDIR);
    closedir MAILDIR;
    foreach my $file (@files) {
        my $filepath = $mailfilepath . $file;
    if ((unlink $filepath) == 0) {
            #filewarn('cannot remove ' . $filepath . ': ' . $!,$logger);
        if (defined $filewarncode and ref $filewarncode eq 'CODE') {
        &$filewarncode('cannot remove ' . $filepath . ': ' . $!,$logger);
        }
        }
    }

}

sub preparemailaddress {

    my ($emailaddress) = @_;
    my $cleanedemailaddress = trim($emailaddress);
    $cleanedemailaddress =~ s/^.*<//g;
    $cleanedemailaddress =~ s/>$//g;
    return $cleanedemailaddress;

}

sub splitrcpts {

    my ($rcptemails) = @_;
    my @rcptemails_arr = ();
    foreach my $rcptemail (split(/;|,/, $rcptemails)) {
        my $cleanedemailaddress = preparemailaddress($rcptemail);
        if (defined $cleanedemailaddress and $cleanedemailaddress ne '') {
            push @rcptemails_arr,$cleanedemailaddress;
        }
    }
    return @rcptemails_arr;

}

sub cleanrcpts {

    my ($rcptemails) = @_;
    if (defined $rcptemails and $rcptemails ne '') {
        return '<' . join('>, <',splitrcpts($rcptemails)) . '>';
    }

}

sub mergercpts {

    my (@rcptemails) = @_;
    return join(',',splitrcpts(join(',',@rcptemails)));

}

sub send_message {

    my ($to, $subject, $message,$fileerrorcode, $emailwarncode) = @_;
    my $errormsg = $mailingdisabled;
    if ($emailenable) {
    $errormsg = send_simple_mail($to,$subject,$message,$sender_address,$system_name, $sender_address,$fileerrorcode, $emailwarncode);
    if ($errormsg != $mailsentsuccessfully ) {
        #emailwarn('error sending email to ' . $to . ' via ' . $smtp_server . ' (' . $errorcode . ')',$_,$logger);
        if (defined $emailwarncode and ref $emailwarncode eq 'CODE') {
          &$emailwarncode('error sending email to ' . $to . ' via ' . $smtp_server,$mailerr_messages->{$errormsg},$_,$logger);
        }
    } else {
            emailinfo('email sent to ' . $to . ' via ' . $smtp_server,$logger);
        }
    }
    return $errormsg;

}

sub send_email {

    my ($email,$attachments,$fileerrorcode, $emailwarncode) = @_;
    my $errormsg = $mailingdisabled;
    if ($emailenable and defined $email) {
    if (not exists $email->{return_path} or not defined $email->{return_path}) {
        $email->{return_path} = $sender_address;
    }

    if (not exists $email->{priority} or not defined $email->{priority}) {
        $email->{priority} = $normalpriority;
    }

    if (not exists $email->{sender_name} or not defined $email->{sender_name}) {
        $email->{sender_name} = $system_name;
    }

    if (not exists $email->{from} or not defined $email->{from}) {
        $email->{from} = $sender_address;
    }

    if (not exists $email->{guid} or not defined $email->{guid}) {
        $email->{guid} = create_guid();
    }

    $errormsg = send_mail_with_attachments($email,$attachments,$fileerrorcode, $emailwarncode);
    if ($errormsg != $mailsentsuccessfully ) {
        #emailwarn('error sending email to ' . mergercpts(($email->{to},$email->{cc},$email->{bcc})) . ' via ' . $smtp_server . ' (' . $errorcode . ')',$_,$logger);
            if (defined $emailwarncode and ref $emailwarncode eq 'CODE') {
          &$emailwarncode('error sending email to ' . mergercpts(($email->{to},$email->{cc},$email->{bcc})) . ' via ' . $smtp_server,$mailerr_messages->{$errormsg},$_,$logger);
        }
    } else {
            emailinfo('email sent to ' . mergercpts(($email->{to},$email->{cc},$email->{bcc})) . ' via ' . $smtp_server,$logger);
        }
    }
    return $errormsg;

}

1;