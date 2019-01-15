use strict;

## no critic

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../../../../../');

use NGCP::BulkProcessor::Projects::Export::Ama::Settings qw(
    update_settings
);

use NGCP::BulkProcessor::LoadConfig qw(
    load_config
    $SIMPLE_CONFIG_TYPE
    $YAML_CONFIG_TYPE
    $ANY_CONFIG_TYPE
);

use NGCP::BulkProcessor::Calendar qw(current_local);

use NGCP::BulkProcessor::Projects::Export::Ama::Format::File qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Record qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013 qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014 qw();

use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::SignificantDigitsNextField qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::DomesticInternational qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime qw();
use NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ElapsedTime qw();

load_config('config.cfg');
load_config('settings.cfg',\&update_settings,$SIMPLE_CONFIG_TYPE);

my $dt = current_local();
my $source = "43011001";
my $destination = "43011002";
my $duration = 123.456;

my $file = NGCP::BulkProcessor::Projects::Export::Ama::Format::File->new(

);

my $limit = 5000;
my $i = 0;
my $file_sequence_number = 123;

sub get_transfer_in {
    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9013->new(

            rewritten => 0,
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),

            file_sequence_number => $file_sequence_number,
        )
    );
}

sub get_record {
    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure0510->new(
            call_type => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::CallType::STATION_PAID,

            rewritten => 0,
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            service_feature => $NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ServiceFeature::OTHER,

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
}

sub get_transfer_out {

    return NGCP::BulkProcessor::Projects::Export::Ama::Format::Record->new(
        NGCP::BulkProcessor::Projects::Export::Ama::Format::Structures::Structure9014->new(

            rewritten => 0,
            sensor_id => '008708', #  Graz

            padding => 0,
            recording_office_id => '008708',

            date => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::Date::get_ama_date($dt),

            connect_time => NGCP::BulkProcessor::Projects::Export::Ama::Format::Fields::ConnectTime::get_connect_time($dt),

            file_sequence_number => $file_sequence_number,

            #=> (scalar @records),
        )
    );

}

sub commit_cb {
    print $file_sequence_number . "\n";
    $file_sequence_number++;
}

while ($i < $limit) {
    $file->write_record(
        get_transfer_in => \&get_transfer_in,
        get_record => \&get_record,
        get_transfer_out => \&get_transfer_out,
        commit_cb => \&commit_cb,
    );
    $i++;
}
$file->close(
    get_transfer_out => \&get_transfer_out,
    commit_cb => \&commit_cb,
);

#print $test->{structure}->to_string()."\n";
#print $test->get_hex();