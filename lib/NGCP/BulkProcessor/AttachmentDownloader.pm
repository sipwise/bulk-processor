# retrieve files from emails

## no critic

package NGCP::BulkProcessor::AttachmentDownloader;
use strict;

use NGCP::BulkProcessor::Logging qw(
    getlogger
    attachmentdownloaderdebug
    attachmentdownloaderinfo
);
use NGCP::BulkProcessor::LogError qw(
    fileerror
    attachmentdownloadererror
    attachmentdownloaderwarn
);

use Email::MIME;
use Email::MIME::Attachment::Stripper;
use URI::Find;
#use File::Fetch;
#use LWP::Simple;
use LWP::UserAgent;
use HTTP::Request;
#use HTTP::Cookies;

use NGCP::BulkProcessor::Utils qw(kbytes2gigs changemod);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    $attachment_no_match
    $attachment_match
    $attachment_found
);

our $attachment_no_match = 0;
our $attachment_match = 1;
our $attachment_found = 2;

#my $logger = getlogger(__PACKAGE__);

sub new {

    my ($class,$derived_class,@params) = @_;
    my $self = bless {}, $derived_class;
    $self->{download_urls} = 0;
    $self->setup(@params);
    return $self;

}

sub setup {

    my $self = shift;
    my (@params) = @_;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub logout {

    my $self = shift;
    my (@params) = @_;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub download {

    my $self = shift;
    my ($filedir) = @_;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));

}

sub _process_message {

    my $self = shift;
    my ($subject,$message_string,$filedir,$files_saved) = @_;


    #if (length($message_string)) {

    attachmentdownloaderinfo('processing message "' . $subject . '"',getlogger(__PACKAGE__));

    my $parsed = Email::MIME->new($message_string);

    my $found = ($self->{download_urls} ? $self->_process_bodies($parsed,$subject,$filedir,$files_saved) : 0);
    $found = $self->_process_attachments($parsed,$subject,$filedir,$files_saved) if !$found;




    #}

    return $found;


}

sub _process_attachments {
    my ($self,$parsed,$subject,$filedir,$files_saved) = @_;

    my $found = 0;

    my $stripper = Email::MIME::Attachment::Stripper->new($parsed, (force_filename => 1));

    my @attachments = $stripper->attachments();

    foreach my $attachment (@attachments) {
    $attachment->{subject} = $subject;
    $attachment->{size} = length($attachment->{payload});
    $attachment->{match} = undef;
    if (defined $self->{checkfilenamecode} and ref $self->{checkfilenamecode} eq 'CODE') {
        my $match = &{$self->{checkfilenamecode}}($attachment);
        if ($match == $attachment_no_match) {
        attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') skipped',getlogger(__PACKAGE__));
        next;
        } elsif ($match == $attachment_found) {
        attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') found',getlogger(__PACKAGE__));
        $found = 1;
        } elsif ($match == $attachment_match) {
        attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') matched',getlogger(__PACKAGE__));
        } else {
        attachmentdownloaderwarn('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') - unknown match, skipped',getlogger(__PACKAGE__));
        next;
        }
    }

    _save_file($attachment,$filedir,$files_saved);


    }
    return $found;
}

sub _save_file {

    my ($attachment,$filedir,$files_saved) = @_;
    my $filepath = $filedir . $attachment->{filename};

    unlink $filepath;

    local *ATTACHMENTFILE;
    if (not open (ATTACHMENTFILE,'>' . $filepath)) {
    fileerror('cannot open file ' . $filepath . ': ' . $!,getlogger(__PACKAGE__));
    return; # $files_saved;
    }
    binmode(ATTACHMENTFILE);
    print ATTACHMENTFILE $attachment->{payload};

    close(ATTACHMENTFILE);
    changemod($filepath);

    push(@$files_saved,{ saved => $filepath, match => $attachment->{match} });

    attachmentdownloaderinfo('attachment saved: ' . $filepath,getlogger(__PACKAGE__));
}

sub _process_bodies {

    my ($self,$parsed,$subject,$filedir,$files_saved) = @_;

    my $found = 0;

    $parsed->walk_parts(sub {
    my ($part) = @_;
    return if $found;
    if ((scalar $part->subparts) > 0) {
        foreach my $subpart ($part->subparts) {
        if (!$found) {
            $found = $self->_process_body($subpart,$subject,$found,$filedir,$files_saved);
        } else {
            last;
        }
        }
    } else {
        $found = $self->_process_body($part,$subject,$found,$filedir,$files_saved);
    }
    });

    return $found;
}

sub _process_body {
    my ($self,$part,$subject,$found,$filedir,$files_saved) = @_;

    if ($part->content_type =~ m/text\//i) {
    my %uris;
    my $finder = URI::Find->new(sub {
        my ($uri,$orig_uri) = @_;
        my $url = $uri->as_string;
        if ($url =~ /^http/i) {
        $uris{$url} = undef;
        }
    });
    my $body = $part->body;
    $finder->find(\$body);
    if ((scalar keys %uris) > 0) {
        foreach my $uri (sort keys %uris) {
        my $attachment = _download_file($uri);
        if ($attachment) {
            $attachment->{subject} = $subject;
            $attachment->{size} = length($attachment->{payload});
            $attachment->{match} = undef;

            if (defined $self->{checkfilenamecode} and ref $self->{checkfilenamecode} eq 'CODE') {
            my $match = &{$self->{checkfilenamecode}}($attachment);
            if ($match == $attachment_no_match) {
                attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') skipped',getlogger(__PACKAGE__));
                next;
            } elsif ($match == $attachment_found) {
                attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') found',getlogger(__PACKAGE__));
                $found = 1;
            } elsif ($match == $attachment_match) {
                attachmentdownloaderinfo('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') matched',getlogger(__PACKAGE__));
            } else {
                attachmentdownloaderwarn('attachment ' . $attachment->{filename} . ' (' . kbytes2gigs(int($attachment->{size} / 1024), undef, 1) . ' ' . $attachment->{content_type} . ') - unknown match, skipped',getlogger(__PACKAGE__));
                next;
            }
            }

            _save_file($attachment,$filedir,$files_saved);
        }
        }
    } else {
        attachmentdownloaderinfo("no urls for download found in part '" . $part->content_type . "'",getlogger(__PACKAGE__));
    }
    }

    return $found;
}

sub _download_file { # .. dropbox links and the like
    my ($uri) = @_;
    my $ua = LWP::UserAgent->new;
    $ua->timeout(10);
    $ua->ssl_opts(
    verify_hostname => 0,
    );
    $ua->cookie_jar({});
    my $request = HTTP::Request->new('GET', $uri);
    attachmentdownloaderinfo('downloading ' . $uri,getlogger(__PACKAGE__));
    my $response = $ua->request($request);
    if ($response->code == 200) {
    my $attachment = {};
    $attachment->{uri} = $uri;
    $attachment->{payload} = $response->decoded_content( charset => 'none' );
    #$attachment->{size} = $response->header('content-length'); # -s $attachment->{payload};
    ($attachment->{filename}) = ($response->header('Content-Disposition') =~ m/"([^"]+)"/);
    return $attachment;
    } else {
    attachmentdownloaderwarn('downloading ' . $uri . ' failed',getlogger(__PACKAGE__));
    }
    return undef;
}

sub DESTROY {

    my $self = shift;
    $self->logout();
}

1;
