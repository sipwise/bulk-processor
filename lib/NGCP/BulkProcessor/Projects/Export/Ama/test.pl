use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use NGCP::BulkProcessor::Calendar qw(current_local);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Record qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime qw();

my $dt = current_local();
my $source = "43011001";
my $destination = "43011002";
my $duration = 123.456;

my $test = NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
    NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510->new(
        call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::STATION_PAID,

        rewritten => 0,
        sensor_id => '008708', #  Graz

        padding => 0,
        recording_office_id => '008708',

        date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

        service_feature_code => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

        originating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($source),
        originating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($source),
        originating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($source),

        domestic_international => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational::get_number_domestic_international($destination),

        terminating_significant_digits => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_length($destination),
        terminating_open_digits_1 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_1($destination),
        terminating_open_digits_2 => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField::get_number_digits_2($destination),

        connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),
        elapsed_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime::get_elapsed_time($duration),
    )
);

print $test->{structure}->to_string()."\n";
print $test->get_hex();

unlink 'test.ama';
if (open(my $fh,">:raw",'test.ama')) {
    print $fh pack('H*','004f0000' . $test->get_hex());
    close $fh;
    #restdebug($self,"$self->{crt_path} saved",getlogger(__PACKAGE__));
} else {
    #fileerror("failed to open $self->{crt_path}: $!",getlogger(__PACKAGE__));
}
