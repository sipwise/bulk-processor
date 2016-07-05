package NGCP::BulkProcessor::RestConnector;
use strict;

## no critic

use URI;
use LWP::UserAgent qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
    restdebug
    restinfo);
use NGCP::BulkProcessor::LogError qw(
    resterror
    restwarn
    restrequesterror
    restresponseerror
    notimplementederror);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw();

#my $logger = getlogger(__PACKAGE__);

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    my $instanceid = shift;

    $self->{instanceid} = $instanceid;
    $self->{tid} = threadid();

    $self->{uri} = undef;
    $self->{netloc} = undef;

    $self->{ua} = undef;

    $self->{req} = undef;
    $self->{res} = undef;
    $self->{requestdata} = undef;
    $self->{responsedata} = undef;

    return $self;

}

sub connectidentifier {

    my $self = shift;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
    return undef;

}

sub baseuri {

    my $self = shift;
    if (@_) {
        my $uri = shift;
        undef $self->{ua};
        undef $self->{uri};
        undef $self->{netloc};
        if (($self->{netloc}) = ($uri =~ m!^https?://(.*)/?.*$!)) {
            $self->{uri} = URI->new($uri);
            $self->{uri}->path_query('');
            $self->{uri}->fragment('');
            restdebug($self,"base URL set to '" . $self->{uri} . "'",getlogger(__PACKAGE__));
        } else {
            resterror($self,"'" . $uri . "' is not a valid URL",getlogger(__PACKAGE__));
        }
    }
    return (defined $self->{uri} ? $self->{uri}->clone() : undef);

}

sub _clearrequestdata {

    my $self = shift;
    $self->{req} = undef;
    $self->{res} = undef;
    $self->{requestdata} = undef;
    $self->{responsedata} = undef;
}

sub _create_ua {

    my $self = shift;
    if (!defined $self->{uri}) {
        resterror($self,'base URL not set',getlogger(__PACKAGE__));
    }
    my $ua = LWP::UserAgent->new();
    $self->_setup_ua($ua,$self->{netloc});
    return $ua;

}

sub _setup_ua {

    my $self = shift;
    my ($ua,$netloc) = @_;
    notimplementederror((ref $self) . ': ' . (caller(0))[3] . ' not implemented',getlogger(__PACKAGE__));
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
        restrequesterror($self,'error executing rest request: ' . $@,$req,$self->{requestdata},getlogger(__PACKAGE__));
    }
    return $res;
}

sub _add_headers {
    my ($req,$headers) = @_;
    foreach my $headername (keys %$headers) {
        $req->header($headername => $headers->{$headername});
    }
}

sub _encode_request_content {
    my $self = shift;
    my ($data) = @_;
    return $data;
}

sub _decode_response_content {
    my $self = shift;
    my ($data) = @_;
    return $data;
}

sub _add_post_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,$headers);
}

sub _encode_post_content {
    my $self = shift;
    my ($data) = @_;
    return $self->_encode_request_content($data);
}

sub _decode_post_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_response_content($data);
}

sub _get_request_uri {
    my $self = shift;
    my ($path_query) = @_;
    if (!defined $self->{uri}) {
        resterror($self,'base URL not set',getlogger(__PACKAGE__));
    }
    if ('URI' eq ref $path_query) {
        $path_query = $path_query->path_query();
    }
    my $uri = $self->{uri}->clone();
    $uri->path_query($path_query);
    return $uri;
}

sub _log_request() {
    my $self = shift;
    my ($req) = @_;
    if ($req) {
        restdebug($self,$req->method . ' ' . $req->uri,getlogger(__PACKAGE__));
    }
}

sub _log_response() {
    my $self = shift;
    my ($res) = @_;
    if ($res) {
        restdebug($self,$res->code . ' ' . $res->message,getlogger(__PACKAGE__));
    }
}

sub post {

    my $self = shift;
    my ($path_query,$data,$headers) = @_;
    $self->_clearrequestdata();
    $self->{requestdata} = $data;
	$self->{req} = HTTP::Request->new('POST',$self->_get_request_uri($path_query));
	$self->_add_post_headers($self->{req},$headers);
	$self->_log_request($self->{req});
	eval {
        $self->{req}->content($self->_encode_post_content($data));
    };
    if ($@) {
        restrequesterror($self,'error encoding POST request content: ' . $@,$self->{req},$data,getlogger(__PACKAGE__));
    }
	$self->{res} = $self->_ua_request($self->{req});
	$self->_log_response($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_post_response($self->{res}->decoded_content());
    };
    if ($@) {
        restresponseerror($self,'error decoding POST response content: ' . $@,$self->{res},getlogger(__PACKAGE__));
    }
	return $self->{res};

}

sub _add_get_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,$headers);
}

sub _decode_get_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_response_content($data);
}

sub get {

    my $self = shift;
    my ($path_query,$headers) = @_;
    $self->_clearrequestdata();
	$self->{req} = HTTP::Request->new('GET',$self->_get_request_uri($path_query));
	$self->_add_get_headers($self->{req},$headers);
	$self->_log_request($self->{req});
	$self->{res} = $self->_ua_request($self->{req});
	$self->_log_response($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_get_response($self->{res}->decoded_content());
    };
    if ($@) {
        restresponseerror($self,'error decoding GET response content: ' . $@,$self->{res},getlogger(__PACKAGE__));
    }
	return $self->{res};

}

sub _add_patch_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,$headers);
}

sub _encode_patch_content {
    my $self = shift;
    my ($data) = @_;
    return $self->_encode_request_content($data);
}

sub _decode_patch_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_response_content($data);
}

sub patch {

    my $self = shift;
    my ($path_query,$data,$headers) = @_;
    $self->_clearrequestdata();
    $self->{requestdata} = $data;
	$self->{req} = HTTP::Request->new('PATCH',$self->_get_request_uri($path_query));
	$self->_add_patch_headers($self->{req},$headers);
	$self->_log_request($self->{req});
	eval {
        $self->{req}->content($self->_encode_patch_content($data));
    };
    if ($@) {
        restrequesterror($self,'error encoding PATCH request content: ' . $@,$self->{req},$data,getlogger(__PACKAGE__));
    }
	$self->{res} = $self->_ua_request($self->{req});
	$self->_log_response($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_patch_response($self->{res}->decoded_content());
    };
    if ($@) {
        restresponseerror($self,'error decoding PATCH response content: ' . $@,$self->{res},getlogger(__PACKAGE__));
    }
	return $self->{res};

}

sub _add_put_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,$headers);
}

sub _encode_put_content {
    my $self = shift;
    my ($data) = @_;
    return $self->_encode_request_content($data);
}

sub _decode_put_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_response_content($data);
}

sub put {

    my $self = shift;
    my ($path_query,$data,$headers) = @_;
    $self->_clearrequestdata();
    $self->{requestdata} = $data;
	$self->{req} = HTTP::Request->new('PUT',$self->_get_request_uri($path_query));
	$self->_add_put_headers($self->{req},$headers);
	$self->_log_request($self->{req});
	eval {
        $self->{req}->content($self->_encode_put_content($data));
    };
    if ($@) {
        restrequesterror($self,'error encoding PUT request content: ' . $@,$self->{req},$data,getlogger(__PACKAGE__));
    }
	$self->{res} = $self->_ua_request($self->{req});
	$self->_log_response($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_put_response($self->{res}->decoded_content());
    };
    if ($@) {
        restresponseerror($self,'error decoding PUT response content: ' . $@,$self->{res},getlogger(__PACKAGE__));
    }
	return $self->{res};

}

sub _add_delete_headers {
    my $self = shift;
    my ($req,$headers) = @_;
    _add_headers($req,$headers);
}

sub _decode_delete_response {
    my $self = shift;
    my ($data) = @_;
    return $self->_decode_response_content($data);
}

sub delete {

    my $self = shift;
    my ($path_query,$headers) = @_;
    $self->_clearrequestdata();
	$self->{req} = HTTP::Request->new('DELETE',$self->_get_request_uri($path_query));
	$self->_add_delete_headers($self->{req},$headers);
	$self->_log_request($self->{req});
	$self->{res} = $self->_ua_request($self->{req});
	$self->_log_response($self->{req});
	eval {
        $self->{responsedata} = $self->_decode_delete_response($self->{res}->decoded_content());
    };
    if ($@) {
        restresponseerror($self,'error decoding DELETE response content: ' . $@,$self->{res},getlogger(__PACKAGE__));
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
