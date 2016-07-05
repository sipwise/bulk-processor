package NGCP::BulkProcessor::RestConnectors::NGCPRestApi;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use JSON qw();

use NGCP::BulkProcessor::Globals qw($LongReadLen_limit);
use NGCP::BulkProcessor::Logging qw(
    getlogger
    restdebug
    restinfo
);
use NGCP::BulkProcessor::LogError qw(
    resterror
    restwarn
    restrequesterror
    restresponseerror);

use NGCP::BulkProcessor::RestConnector;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::RestConnector);
our @EXPORT_OK = qw();

my $defaulturi = 'https://127.0.0.1:443';
my $defaultusername = 'administrator';
my $defaultpassword = 'administrator';
my $defaultrealm = 'api_admin_http';

my $contenttype = 'application/json';
my $patchcontenttype = 'application/json-patch+json';

#my $logger = getlogger(__PACKAGE__);

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::RestConnector->new(@_);

    baseuri(shift // $defaulturi);
    $self->{username} = shift;
    $self->{password} = shift;
    $self->{realm} = shift // $defaultrealm;

    bless($self,$class);

    restdebug($self,__PACKAGE__ . ' connector created',getlogger(__PACKAGE__));

    return $self;

}

sub connectidentifier {

    my $self = shift;
    return $self->{username} . '@' . $self->{uri};

}

sub _setup_ua {

    my $self = shift;
    my ($ua,$netloc) = @_;
    $ua->ssl_opts(
		verify_hostname => 0,
		SSL_verify_mode => 0,
	);
    $ua->credentials($netloc, $self->{realm}, $self->{username}, $self->{password});

}

sub _encode_request_content {
    my $self = shift;
    my ($data) = @_;
    return JSON::to_json($data);
}

sub _decode_response_content {
    my $self = shift;
    my ($data) = @_;
    return JSON::from_json($data);
}

sub _add_post_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,{
       'Content-Type' => $contenttype,
    });
    # allow providing custom headers to post(),
    # e.g { 'X-Fake-Clienttime' => ... }
    $self->SUPER::_add_post_headers($req,$headers);
}

sub _add_get_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    $self->SUPER::_add_get_headers($req,$headers);
}

sub _add_patch_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,{
       'Prefer' => 'return=representation',
       'Content-Type' => $patchcontenttype,
    });
	$self->SUPER::_add_patch_headers($req,$headers);
}

sub _encode_patch_content {
    my $self = shift;
    my ($data) = @_;
    return JSON::to_json(
		[ map { local $_ = $_; { op => 'replace', path => '/'.$_ , value => $data->{$_} }; } keys %$data ]
	);
}

sub _add_put_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,{
       'Prefer' => 'return=representation',
       'Content-Type' => $contenttype,
    });
	$self->SUPER::_add_put_headers($req,$headers);
}

sub _add_delete_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    $self->SUPER::_add_delete_headers($req,$headers);
}

1;
