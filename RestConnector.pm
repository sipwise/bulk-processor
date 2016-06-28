package RestConnector;
use strict;

## no critic

use threads;
use threads::shared;

use Logging qw(
    getlogger
    restdebug
    restinfo);
use LogError qw(
    resterror
    restwarn
    notimplementederror);

use Utils qw(threadid);
#use Array qw(arrayeq);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(xxx);

my $logger = getlogger(__PACKAGE__);

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my $instanceid = shift;
    my $uri = shift;

    $self->{instanceid} = $instanceid;
    $self->{tid} = threadid();

    $self->{uri} = $uri // $self->_get_base_uri();
    if (($self->{netloc}) = ($self->{uri} =~ m!^https?://(.*)/?.*$!)) {
        resterror("'" . $self->{uri} . "' is not a valid URL",$logger);
    }

    $self->{ua} = undef;
    
    $self->{req} = undef;
    $self->{res} = undef;
    $self->{requestdata} = undef;
    $self->{responsedata} = undef;

    return $self;

}

sub _clear {

    my $self = shift;
    $self->{req} = undef;
    $self->{res} = undef;
    $self->{requestdata} = undef;
    $self->{responsedata} = undef;
}

sub _get_base_uri {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',$logger);
    return undef;

}

sub _create_ua {

    my $self = shift;
    $ua = LWP::UserAgent->new();
    $self->_setup_ua($ua,$self->{netloc});
    return $ua;

}

sub _setup_ua {

    my $self = shift;
    my ($ua,$netloc) = @_;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',$logger);
    return undef;

}

sub _get_ua {
    my $self = shift;
    if (!defined $self->{ua}) {
        $self->{ua} = $self->_create_ua();
    }
    return $self->{ua};
}

sub _ua_request {
    my $self = shift;
    my ($req)= @_;
    my $res = undef;
    eval {
        $res = $self->_get_ua()->request($req);
    };
    if ($@) {
        resterror('error executing rest request: ' . $@,$logger);
    }
    return $res;
}

sub _encode_request_content {
    my $self = shift;
    my ($data) = @_;
    return $data;
}

sub _decode_request_response {
    my $self = shift;
    my ($data) = @_;
    return $data;
}

sub _add_post_headers {
    my $self = shift;
    my ($req) = @_;
}

sub _encode_post_content {
    my $self = shift;
    my ($data) = @_;
    return $self->_encode_request_content($data);
}

sub _decode_post_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_request_content($data);
}

sub post {

    my $self = shift;
    my ($uripart,$data) = @_;
    $self->_clear();
    $self->{requestdata} = $data;
	$self->{req} = HTTP::Request->new('POST', $self->{uri} . $uripart);
	_add_post_headers($self->{req});
	#$req->header('Content-Type' => 'application/json');
	#$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	eval {
        $self->{req}->content($self->_encode_post_content($data));
    };
    if ($@) {
        resterror('error encoding POST request content: ' . $@,$logger);
    }
	$self->{res} = $self->_ua_request($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_post_response($self->{res}->decoded_content());
    };
    if ($@) {
        resterror('error decoding POST response content: ' . $@,$logger);
    }
	return $self->{res};
	
}

sub _add_get_headers {
    my $self = shift;
    my ($req) = @_;
}

sub _decode_get_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_request_content($data);
}

sub get {

    my $self = shift;
    my ($uripart) = @_;
    $self->_clear();
	$self->{req} = HTTP::Request->new('GET', $self->{uri} . $uripart);
	_add_get_headers($self->{req});
	#$req->header('Content-Type' => 'application/json');
	#$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	$self->{res} = $self->_ua_request($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_get_response($self->{res}->decoded_content());
    };
    if ($@) {
        resterror('error decoding GET response content: ' . $@,$logger);
    }	
	return $self->{res};
	
}

sub _add_patch_headers {
    my $self = shift;
    my ($req) = @_;
}

sub _encode_patch_content {
    my $self = shift;
    my ($data) = @_;
    return $self->_encode_request_content($data);
}

sub _decode_patch_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_request_content($data);
}

sub patch {

    my $self = shift;
    my ($uripart,$data) = @_;
    $self->_clear();
    $self->{requestdata} = $data;
	$self->{req} = HTTP::Request->new('PATCH', $self->{uri} . $uripart);
	_add_patch_headers($self->{req});
	#$req->header('Content-Type' => 'application/json');
	#$req->header('X-Fake-Clienttime' => _get_fake_clienttime_now());
	eval {
        $self->{req}->content($self->_encode_patch_content($data));
    };
    if ($@) {
        resterror('error encoding PATCH request content: ' . $@,$logger);
    }
	$self->{res} = $self->_ua_request($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_patch_response($self->{res}->decoded_content());
    };
    if ($@) {
        resterror('error decoding PATCH response content: ' . $@,$logger);
    }
	return $self->{res};
	
}


sub instanceidentifier {
    my $self = shift;

    $self->{instanceid} = shift if @_;
    return $self->{instanceid};

}

sub request {
    my $self = shift;
    return $self->{req};
}

sub response {
    my $self = shift;
    return $self->{res};
}

sub requestdata {
    my $self = shift;
    $self->{requestdata} = shift if @_;
    return $self->{requestdata};
}

sub responsedata {
    my $self = shift;
    $self->{responsedata} = shift if @_;
    return $self->{responsedata};
}

1;