package EWS::Client;
use strict;
use warnings;
BEGIN {
  $EWS::Client::VERSION = '1.300000';
}
use Moose;

with qw/
    EWS::Client::Role::SOAP
    EWS::Client::Role::GetItem
    EWS::Client::Role::GetAttachment
    EWS::Client::Role::FindItem
    EWS::Client::Role::FindFolder
    EWS::Client::Role::GetFolder
    EWS::Client::Role::ExpandDL
    EWS::Client::Role::GetUserAvailability
    EWS::Client::Role::ResolveNames
/;
use EWS::Client::Contacts;
use EWS::Client::Calendar;
use EWS::Client::Folder;
use EWS::Client::DistributionList;
use URI::Escape ();
use Log::Report;

has username => (
    is => 'rw',
    isa => 'Maybe[Str]',
    required => 0,
);

has password => (
    is => 'rw',
    isa => 'Maybe[Str]',
    required => 0,
);

has server => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has contacts => (
    is => 'ro',
    isa => 'EWS::Client::Contacts',
    lazy_build => 1,
);

sub _build_contacts {
    my $self = shift;
    return EWS::Client::Contacts->new({ client => $self });
}

has calendar => (
    is => 'ro',
    isa => 'EWS::Client::Calendar',
    lazy_build => 1,
);

sub _build_calendar {
    my $self = shift;
    return EWS::Client::Calendar->new({ client => $self });
}

has folders => (
    is => 'ro',
    isa => 'EWS::Client::Folder',
    lazy_build => 1,
);

sub _build_folders {
    my $self = shift;
    return EWS::Client::Folder->new({ client => $self });
}

has distribution_list => (
    is => 'ro',
    isa => 'EWS::Client::DistributionList',
    lazy_build => 1,
);

sub _build_distribution_list {
    my $self = shift;
    return EWS::Client::DistributionList->new({ client => $self });
}

sub BUILDARGS {
    my ($class, @rest) = @_;
    my $params = (scalar @rest == 1 ? $rest[0] : {@rest});

    # collect EWS password from environment as last resort
    $params->{password} ||= $ENV{EWS_PASS};

    return $params;
}

sub BUILD {
    my ($self, $params) = @_;

    if ($self->use_negotiated_auth) {
        die "please install LWP::Authen::Ntlm"
            unless eval { require LWP::Authen::Ntlm && $LWP::Authen::Ntlm::VERSION };
        die "please install Authen::NTLM"
            unless eval { require Authen::NTLM && $Authen::NTLM::VERSION };

        # change email style username to win-domain style
        if ($self->username =~ m/(.+)@(.+)/) {
            $self->username( $2 .'\\'. $1 );
        }
    } else {
        # URI escape the username and password
        $self->password( URI::Escape::uri_escape($self->password) );
        $self->username( URI::Escape::uri_escape($self->username) );
    }
}

__PACKAGE__->meta->make_immutable;
no Moose;
1;