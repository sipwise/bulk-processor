package NGCP::BulkProcessor::Projects::Export::Ama::Ccs::Settings;
use strict;

## no critic

use NGCP::BulkProcessor::Globals qw(
    $working_path
    $enablemultithreading
    $cpucount
    create_path
);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    scriptinfo
    configurationinfo
);

use NGCP::BulkProcessor::LogError qw(
    fileerror
    filewarn
    configurationwarn
    configurationerror
);

use NGCP::BulkProcessor::LoadConfig qw(
    split_tuple
    parse_regexp
);
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits); #stringtobool
#format_number check_ipnet
use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings

    $input_path

    $defaultsettings
    $defaultconfig


    $skip_errors
    $force

    $export_cdr_multithreading
    $export_cdr_numofthreads
    $export_cdr_blocksize
    $export_cdr_joins
    $export_cdr_conditions
    $export_cdr_limit
    $export_cdr_stream
    $export_cdr_rollover_fsn

    $ama_sensor_id
    $ama_recording_office_id
    $ama_incoming_trunk_group_number
    $ama_outgoing_trunk_group_number
    $ama_originating_digits_cdr_field
    $ama_terminating_digits_cdr_field

    @ivr_u2u_headers
    $primary_alias_pattern

    $switch_number_pattern
    $switch_number_replacement

    $originating_pattern
    $originating_replacement

    $terminating_pattern
    $terminating_replacement

    $terminating_open_digits_6001
);
#$ivr_duration_limit

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $input_path = $working_path . 'input/';

our $force = 0;
#our $dry = 0;
our $skip_errors = 0;

our $export_cdr_multithreading = $enablemultithreading;
our $export_cdr_numofthreads = $cpucount;
our $export_cdr_blocksize = undef;
our $export_cdr_joins = [];
our $export_cdr_conditions = [];
our $export_cdr_limit = undef;
our $export_cdr_stream = undef;
our $export_cdr_rollover_fsn = 0;

our $ama_sensor_id;
our $ama_recording_office_id;
our $ama_incoming_trunk_group_number;
our $ama_outgoing_trunk_group_number;
our $ama_originating_digits_cdr_field;
our $ama_terminating_digits_cdr_field;

#our $ivr_duration_limit = 5;
our @ivr_u2u_headers = ();
our $primary_alias_pattern = undef;
our $switch_number_pattern = undef;
our $switch_number_replacement = undef;
our $terminating_open_digits_6001 = undef;
our $originating_pattern = undef;
our $originating_replacement = undef;
our $terminating_pattern = undef;
our $terminating_replacement = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        $result &= _prepare_working_paths(1);

        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $export_cdr_multithreading = $data->{export_cdr_multithreading} if exists $data->{export_cdr_multithreading};
        $export_cdr_numofthreads = _get_numofthreads($cpucount,$data,'export_cdr_numofthreads');
        $export_cdr_blocksize = $data->{export_cdr_blocksize} if exists $data->{export_cdr_blocksize};

        my $parse_result;
        ($parse_result,$export_cdr_joins) = _parse_export_joins($data->{export_cdr_joins},$configfile);
        $result &= $parse_result;
        ($parse_result,$export_cdr_conditions) = _parse_export_joins($data->{export_cdr_conditions},$configfile);
        $result &= $parse_result;

        $export_cdr_limit = $data->{export_cdr_limit} if exists $data->{export_cdr_limit};
        $export_cdr_stream = $data->{export_cdr_stream} if exists $data->{export_cdr_stream};
        $export_cdr_rollover_fsn = $data->{export_cdr_rollover_fsn} if exists $data->{export_cdr_rollover_fsn};

        $ama_sensor_id = $data->{ama_sensor_id} if exists $data->{ama_sensor_id};
        $ama_recording_office_id = $data->{ama_recording_office_id} if exists $data->{ama_recording_office_id};
        $ama_incoming_trunk_group_number = $data->{ama_incoming_trunk_group_number} if exists $data->{ama_incoming_trunk_group_number};
        $ama_outgoing_trunk_group_number = $data->{ama_outgoing_trunk_group_number} if exists $data->{ama_outgoing_trunk_group_number};

        $ama_originating_digits_cdr_field = $data->{ama_originating_digits_cdr_field} if exists $data->{ama_originating_digits_cdr_field};
        unless (contains($ama_originating_digits_cdr_field,[qw(source_user source_user_out source_cli)])) {
            configurationerror($configfile,'unknown ama_originating_digits_cdr_field',getlogger(__PACKAGE__));
        }
        $ama_terminating_digits_cdr_field = $data->{ama_terminating_digits_cdr_field} if exists $data->{ama_terminating_digits_cdr_field};
        unless (contains($ama_terminating_digits_cdr_field,[qw(destination_user destination_user_out destination_user_dialed destination_user_in)])) {
            configurationerror($configfile,'unknown ama_terminating_digits_cdr_field',getlogger(__PACKAGE__));
        }

        #$ivr_duration_limit = $data->{ivr_duration_limit} if exists $data->{ivr_duration_limit};
        @ivr_u2u_headers = split_tuple($data->{ivr_u2u_headers}) if exists $data->{ivr_u2u_headers};

        my $regexp_result;
        $primary_alias_pattern = $data->{primary_alias_pattern} if exists $data->{primary_alias_pattern};
        ($regexp_result,$primary_alias_pattern) = parse_regexp($primary_alias_pattern,$configfile);
        $result &= $regexp_result;

        $switch_number_pattern = $data->{switch_number_pattern} if exists $data->{switch_number_pattern};
        ($regexp_result,$switch_number_pattern) = parse_regexp($switch_number_pattern,$configfile);
        $result &= $regexp_result;

        $switch_number_replacement = $data->{switch_number_replacement} if exists $data->{switch_number_replacement};

        $originating_pattern = $data->{originating_pattern} if exists $data->{originating_pattern};
        ($regexp_result,$originating_pattern) = parse_regexp($originating_pattern,$configfile);
        $result &= $regexp_result;

        $originating_replacement = $data->{originating_replacement} if exists $data->{originating_replacement};

        $terminating_pattern = $data->{terminating_pattern} if exists $data->{terminating_pattern};
        ($regexp_result,$terminating_pattern) = parse_regexp($terminating_pattern,$configfile);
        $result &= $regexp_result;

        $terminating_replacement = $data->{terminating_replacement} if exists $data->{terminating_replacement};

        $terminating_open_digits_6001 = $data->{terminating_open_digits_6001} if exists $data->{terminating_open_digits_6001};

        return $result;

    }
    return 0;

}

sub _prepare_working_paths {

    my ($create) = @_;
    my $result = 1;
    my $path_result;

    ($path_result,$input_path) = create_path($working_path . 'input',$input_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

sub _parse_export_joins {
    my ($token,$file) = @_;
    my @joins = ();
    if (defined $token and length($token) > 0) {
        foreach my $f (_split(\$token)) {
            next unless($f);
            $f =~ s/^\s*\{?\s*//;
            $f =~ s/\}\s*\}\s*$/}/;
            my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
            $a =~ s/^\s*\'//;
            $a =~ s/\'$//g;
            $b =~ s/\s*\}\s*$//;
            my ($c, $d) = split(/\s*=>\s*/, $b);
            $c =~ s/^\s*\'//g;
            $c =~ s/\'\s*//;
            $d =~ s/^\s*\'//g;
            $d =~ s/\'\s*//;
            push @joins, { $a => { $c => $d } };
        }
    }
    return (1,\@joins);
}

sub _parse_export_conditions {
    my ($token,$file) = @_;
    my @conditions = ();
    if (defined $token and length($token) > 0) {
        foreach my $f (_split(\$token)) {
            next unless($f);
            $f =~ s/^\s*\{?\s*//;
            $f =~ s/\}\s*\}\s*$/}/;
            my ($a, $b) = split(/\s*=>\s*{\s*/, $f);
            $a =~ s/^\s*\'//;
            $a =~ s/\'$//g;
            $b =~ s/\s*\}\s*$//;
            my ($c, $d) = split(/\s*=>\s*/, $b);
            $c =~ s/^\s*\'//g;
            $c =~ s/\'\s*//;
            $d =~ s/^\s*\'//g;
            $d =~ s/\'\s*//;
            push @conditions, { $a => { $c => $d } };
        }
    }
    return (1,\@conditions);
}

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $_numofthreads = $default_value;
    $_numofthreads = $data->{$key} if exists $data->{$key};
    $_numofthreads = $cpucount if $_numofthreads > $cpucount;
    return $_numofthreads;
}

sub _split {

    my $buffer_ref = shift;
    my $pos = 0;
    my @tokens = ();
    my $is_literal = 0;
    my $token = '';
    while ($pos < length($$buffer_ref)) {
        if ("'" eq substr($$buffer_ref,$pos,length("'"))) {
            $is_literal = not $is_literal;
            $token .= "'";
            $pos += length("'");
        } elsif ("," eq substr($$buffer_ref,$pos,length(","))) {
            if ($is_literal) {
                $token .= ",";
            } else {
                push(@tokens,$token);
                $token = '';
            }
            $pos += length(",");
        } else {
            $token .= substr($$buffer_ref,$pos,1);
            $pos += 1;
        }
    }
    push(@tokens,$token);
    return @tokens;
}

1;
