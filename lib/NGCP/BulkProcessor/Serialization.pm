package NGCP::BulkProcessor::Serialization;
use strict;

## no critic

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    serialize
    deserialize
    serialize_storable
    deserialize_storable
    serialize_xml
    deserialize_xml
    serialize_yaml
    deserialize_yaml
    serialize_json
    deserialize_json
    serialize_php
    deserialize_php
    serialize_perl
    deserialize_perl
    serialize_storable_base64
    deserialize_storable_base64

    $format_xml
    $format_yaml
    $format_json
    $format_php
    $format_perl
    $format_storable_base64
);
                      #$format_storable

#our $format_storable = 0;
our $format_xml = 1;
our $format_yaml = 2;
our $format_json = 3;
our $format_php = 4;
our $format_perl = 5;
our $format_storable_base64 = 6;

use MIME::Base64 qw(encode_base64 decode_base64);

#http://blogs.perl.org/users/steven_haryanto/2010/09/comparison-of-perl-serialization-modules.html
use Storable; # qw( nfreeze thaw );

use JSON -support_by_pp, -no_export;
#use JSON::XS; # qw(encode_json decode_json);

use Data::Dump; # qw(dump);
$Data::Dump::INDENT = '  ';
$Data::Dump::TRY_BASE64 = 0;

#use YAML::Syck qw(Dump Load);
#$YAML::Syck::ImplicitTyping = 1;

use YAML::XS; # qw(Dump Load);
$YAML::XS::UseCode = 0;
$YAML::XS::DumpCode = 0;
$YAML::XS::LoadCode = 0;
$YAML::XS::QuoteNumericStrings = 1;

use XML::Dumper 0.81;
my $errorcontext = undef; #undef to disable
my $protocolencoding = 'ISO-8859-1';

use PHP::Serialization; #qw(serialize unserialize);

#encrypted:
#use Data::Serializer;
#my $serializer = Data::Serializer->new();
#$serializer = Data::Serializer->new(
#                          serializer => 'Storable',
#                          digester   => 'MD5',
#                          cipher     => 'DES',
#                          secret     => 'my secret',
#                          compress   => 1,
#                        );

#$serialized = $obj->serialize({a => [1,2,3],b => 5});
#$deserialized = $obj->deserialize($serialized);


sub serialize {
    my ($input_ref,$format) = @_;
    if ($format == $format_xml) {
        return serialize_xml($input_ref);
    } elsif ($format == $format_yaml) {
        return serialize_yaml($input_ref);
    } elsif ($format == $format_json) {
        return serialize_json($input_ref);
    } elsif ($format == $format_php) {
        return serialize_php($input_ref);
    } elsif ($format == $format_perl) {
        return serialize_perl($input_ref);
    } elsif ($format == $format_storable_base64) {
        return serialize_storable_base64($input_ref);
    } else { #$format_storable
        return serialize_storable($input_ref);
    }
}

sub deserialize {
    my ($input_ref,$format) = @_;
    if ($format == $format_xml) {
        return deserialize_xml($input_ref);
    } elsif ($format == $format_yaml) {
        return deserialize_yaml($input_ref);
    } elsif ($format == $format_json) {
        return deserialize_json($input_ref);
    } elsif ($format == $format_php) {
        return deserialize_php($input_ref);
    } elsif ($format == $format_perl) {
        return deserialize_perl($input_ref);
    } elsif ($format == $format_storable_base64) {
        return deserialize_storable_base64($input_ref);
    } else { #$format_storable
        return deserialize_storable($input_ref);
    }
}

sub serialize_storable {
    my $input_ref = shift;
    return Storable::nfreeze($input_ref);
}
sub deserialize_storable {
    my $input_ref = shift;
    return Storable::thaw($input_ref);
}

sub serialize_storable_base64 {
    my $input_ref = shift;
    return encode_base64(Storable::nfreeze($input_ref),'');
}
sub deserialize_storable_base64 {
    my $input_ref = shift;
    return Storable::thaw(decode_base64($input_ref));
}

sub _get_xml_dumper {
  my $xml_dumper;
  my %xml_parser_params = ();
  if ($errorcontext) {
    $xml_parser_params{ErrorContext} = $errorcontext;
    #$xml_dumper = XML::Dumper->new(ErrorContext => $errorcontext,ProtocolEncoding => $protocolencoding);
  #} else {
    #$xml_dumper = XML::Dumper->new(ProtocolEncoding => $protocolencoding);
  }
  $xml_parser_params{ProtocolEncoding} = $protocolencoding;
  $xml_dumper = XML::Dumper->new(%xml_parser_params);
  #$xml_dumper->{xml_parser_params} = \%xml_parser_params;
  $xml_dumper->dtd();

  return $xml_dumper;
}

sub serialize_xml {
    my $input_ref = shift;
    return _get_xml_dumper()->pl2xml($input_ref);
}

sub deserialize_xml {
    my $input_ref = shift;
    return _get_xml_dumper()->xml2pl($input_ref);
}

sub serialize_json {
    my $input_ref = shift;
    #return JSON::XS::encode_json($input_ref);
    return JSON::to_json($input_ref, { allow_nonref => 1, allow_blessed => 1, convert_blessed => 1, pretty => 0 });
}

sub deserialize_json {
    my $input_ref = shift;
    #return JSON::XS::decode_json($input_ref);
    return JSON::from_json($$input_ref, { allow_nonref => 1, });
}

sub serialize_yaml {
    my $input_ref = shift;
    return YAML::XS::Dump($input_ref);
}

sub deserialize_yaml {
    my $input_ref = shift;
    return YAML::XS::Load($input_ref);
}


sub serialize_php {
    my $input_ref = shift;
    return PHP::Serialization::serialize($input_ref);
}

sub deserialize_php {
    my $input_ref = shift;
    return PHP::Serialization::unserialize($input_ref);
}

sub serialize_perl {
    my $input_ref = shift;
    return Data::Dump::dump($input_ref);
}

sub deserialize_perl {
    my $input_ref = shift;
    my $data = eval $input_ref;
    if ($@) {
        die($@);
    } else {
        return $data;
    }
}

1;
