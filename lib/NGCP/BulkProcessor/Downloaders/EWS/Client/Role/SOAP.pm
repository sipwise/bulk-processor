package EWS::Client::Role::SOAP;
use strict;
use warnings;
BEGIN {
  $EWS::Client::Role::SOAP::VERSION = '1.143070';
}
use Moose::Role;

use XML::Compile::WSDL11;
use XML::Compile::SOAP11;
use XML::Compile::Transport::SOAPHTTP;
use File::ShareDir ();
use LWP::Authen::OAuth2 qw();

has server_version => (
    is => 'ro',
    isa => 'Str',
    default => 'Exchange2007_SP1',
    required => 0,
);

has use_negotiated_auth => (
    is => 'ro',
    isa => 'Any',
    default => 0,
    required => 0,
);

has tenant_id => (
    is => 'ro',
    isa => 'Str',
    default => undef,
    required => 0,
);

has client_id => (
    is => 'ro',
    isa => 'Str',
    default => undef,
    required => 0,
);

has client_secret => (
    is => 'ro',
    isa => 'Str',
    default => undef,
    required => 0,
);

has transporter => (
    is => 'ro',
    isa => 'XML::Compile::Transport::SOAPHTTP',
    lazy_build => 1,
);

sub _build_transporter {
    my $self = shift;
    my $addr = $self->server . '/EWS/Exchange.asmx';

    if (not $self->use_negotiated_auth) {
        $addr = sprintf '%s:%s@%s',
            $self->username, $self->password, $addr;
    }

    my $t = XML::Compile::Transport::SOAPHTTP->new(
        address => 'https://'. $addr,
        user_agent => $self->_create_oauth_ua(),
    );
    
    if ($self->use_negotiated_auth) {
        $t->userAgent->credentials($self->server.':443', '',
            $self->username, $self->password);
    }

    # XXX disable all security checks
    #$t->userAgent->ssl_opts( verify_hostname => 0, SSL_verify_mode => 0x00 );

    return $t;
}

has wsdl => (
    is => 'ro',
    isa => 'XML::Compile::WSDL11',
    lazy_build => 1,
);

sub _build_wsdl {
    my $self = shift;

    XML::Compile->addSchemaDirs( $self->schema_path );
    my $wsdl = XML::Compile::WSDL11->new('ews-services.wsdl');
    $wsdl->importDefinitions('ews-types.xsd');
    $wsdl->importDefinitions('ews-messages.xsd');

    # skip the t:Culture element in the ResolveNames response
    # it breaks the XML Parser for some reason
    $wsdl->addHook(path => "{http://schemas.microsoft.com/exchange/services/2006/messages}ResolveNamesResponse/ResponseMessages/ResolveNamesResponseMessage/ResolutionSet/Resolution/Contact",
        before => sub {
            my ($xml, $path) = @_;
            my @nodes = $xml->childNodes();
            foreach my $node (@nodes) {
                if($node->nodeName eq 't:Culture'){
                    $xml->removeChild($node);
                }
            }
            return $xml;
         });

    return $wsdl;
}

has schema_path => (
    is => 'ro',
    isa => 'Str',
    lazy_build => 1,
);

sub _build_schema_path {
    my $self = shift;
    return File::ShareDir::dist_dir('EWS-Client');
}

sub _create_oauth_ua {

    my $self = shift;
	
	my $tenant_id = $self->tenand_id;
	return unless $tenant_id;

    #https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize?
    #client_id=6731de76-14a6-49ae-97bc-6eba6914391e
    #&response_type=code%20id_token
    #&redirect_uri=http%3A%2F%2Flocalhost%2Fmyapp%2F
    #&response_mode=fragment
    #&scope=openid%20offline_access%20https%3A%2F%2Fgraph.microsoft.com%2Fuser.read
    #&state=12345
    #&nonce=abcde
    #&code_challenge=YTFjNjI1OWYzMzA3MTI4ZDY2Njg5M2RkNmVjNDE5YmEyZGRhOGYyM2IzNjdmZWFhMTQ1ODg3NDcxY2Nl
    #&code_challenge_method=S256

    my $oauth2 = LWP::Authen::OAuth2->new(
        client_id => $self->client_id, 
        client_secret => $self->client_secret, 
        #service_provider => "Google",
        redirect_uri => undef,
        #client_type => "application",
        authorization_required_params => [ 'client_id', ],#'response_type', ], # 'response_mode', 'state', ], # 'nonce', 'code_challenge', 'code_challenge_method' ],
        is_strict => 0,
        authorization_default_params => {
			#response_type => 'code id_token',
			#response_mode => 'fragment',
			#state => '12345',
			#nonce => 'abcde',
			#code_challenge => 'YTFjNjI1OWYzMzA3MTI4ZDY2Njg5M2RkNmVjNDE5YmEyZGRhOGYyM2IzNjdmZWFhMTQ1ODg3NDcxY2Nl',
			#code_challenge_method => 'S256',
		},
     
        # Optional hook, but recommended.
        save_tokens => \&save_tokens,
        authorization_endpoint => "https://login.microsoftonline.com/$tenant_id/oauth2/v2.0/authorize", #"https://login.microsoftonline.com/$tenant_id/v2.0",
        token_endpoint => "https://login.microsoftonline.com/$tenant_id/oauth2/v2.0/token", #"https://login.microsoftonline.com/$tenant_id/v2.0",
    
        # This is for when you have tokens from last time.
        # token_string => $token_string.
    );

    #my $url = $oauth2->authorization_url();
    #print $url;
    $oauth2->request_tokens(
        #response_mode=>"query",
        #scope=>"https://graph.microsoft.com/mail.read/.default", #"openid offline_access",
        #scope=>"https://outlook.office.com/EWS.AccessAsUser.All/.default",
        scope=>"https://outlook.office.com/.default", 
        #scope=>"EWS.AccessAsUser.All",
        #redirect_uri=>"http://localhost/myapp/",
        grant_type=>'client_credentials',
        #grant_type=>'authorization_code',
        #code_verifier=>'ThisIsntRandomButItNeedsToBe43CharactersLong',
    );

	#client_id=6731de76-14a6-49ae-97bc-6eba6914391e
	#&scope=https%3A%2F%2Fgraph.microsoft.com%2Fmail.read
	#&code=OAAABAAAAiL9Kn2Z27UubvWFPbm0gLWQJVzCTE9UkP3pSx1aXxUjq3n8b2JRLk4OxVXr...
	#&redirect_uri=http%3A%2F%2Flocalhost%2Fmyapp%2F
	#&grant_type=authorization_code
	#&code_verifier=ThisIsntRandomButItNeedsToBe43CharactersLong 
	#&client_secret=JqQX2PNo9bpM0uEihUPzyrh    // NOTE: Only required for web apps. This secret needs to be URL-Encoded.
	return $oauth2;
}

sub save_tokens {
    my ($token_string) = @_;
 
    print "token:" . $token_string;
}

no Moose::Role;
1;
