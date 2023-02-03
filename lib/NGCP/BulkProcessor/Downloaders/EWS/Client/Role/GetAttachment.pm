package EWS::Client::Role::GetAttachment;
use strict;
use warnings;
BEGIN {
  $EWS::Client::Role::GetAttachment::VERSION = '1.143070';
}
use Moose::Role;
 
has GetAttachment => (
    is => 'ro',
    isa => 'CodeRef',
    lazy_build => 1,
);
 
sub _build_GetAttachment {
    my $self = shift;
    return $self->wsdl->compileClient(
        operation => 'GetAttachment',
        transport => $self->transporter->compileClient(
            action => 'http://schemas.microsoft.com/exchange/services/2006/messages/GetAttachment' ),
    );
}
 
no Moose::Role;
1;