package NGCP::BulkProcessor::Projects::ETL::Customer::Settings;
use strict;

## no critic

use threads::shared qw();

use File::Basename qw(fileparse);
use NGCP::BulkProcessor::Serialization qw();
use DateTime::TimeZone qw();

use JSON -support_by_pp, -no_export;
*NGCP::BulkProcessor::Serialization::serialize_json = sub {
    my $input_ref = shift;
    return JSON::to_json($input_ref, { allow_nonref => 1, allow_blessed => 1, convert_blessed => 1, pretty => 1, as_nonblessed => 1 });
};

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

use NGCP::BulkProcessor::Utils qw(prompt timestampdigits threadid load_module);

use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    run_dao_method
    get_dao_var
    get_export_filename
    write_export_file
    write_sql_file

    update_load_recursive
    $load_yml
    $load_recursive
    
    update_tabular_fields
    $tabular_yml
    $tabular_fields
    $ignore_tabular_unique
    $tabular_single_row_txn
    $graph_yml
    $graph_fields
    $graph_fields_mode
    update_graph_fields

    $sqlite_db_file
    $csv_dir

    check_dry

    $output_path
    $input_path

    $customer_export_filename_format
    $customer_import_filename
    $split_customers

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $export_customers_multithreading
    $export_customers_numofthreads
    $export_customers_blocksize
   
    $csv_all_expected_fields
);
#$cf_default_priority
#$cf_default_timeout
#$cft_default_ringtimeout

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $tabular_yml = 'tabular.yml';
our $tabular_fields = [];
our $ignore_tabular_unique = 0;
our $tabular_single_row_txn = 1;

our $graph_yml = 'graph.yml';
our $graph_fields = [];
our $graph_fields_mode = 'whitelist';
my @graph_fields_modes = qw(whitelist blacklist);

our $load_yml = 'load.yml';
our $load_recursive;

our $output_path = $working_path . 'output/';
our $input_path = $working_path . 'input/';
our $csv_dir = 'customer';

our $customer_export_filename_format = undef;

our $csv_all_expected_fields = 1;

#our $customer_import_filename = undef;
#our $customer_import_numofthreads = $cpucount;
#our $customer_import_multithreading = 1;
#our $customer_reseller_name = 'default';
#our $customer_billing_profile_name = 'Default Billing Profile';
#our $customer_domain = undef;
#our $customer_contact_email_format = '%s@example.org';
#our $subscriber_contact_email_format = '%s@example.org';
#our $split_customers = 0;

#our $subscriber_timezone = undef;
#our $contract_timezone = undef;

#our $subscriber_profile_set_name = undef;
#our $subscriber_profile_name = undef;
#our $webusername_format = '%1$s';
#our $subscriber_externalid_format = undef;

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

my $mr = 'Trunk';
my @supported_mr = ('Trunk');

our $sqlite_db_file = 'sqlite';

our $export_customers_multithreading = $enablemultithreading;
our $export_customers_numofthreads = $cpucount;
our $export_customers_blocksize = 1000;

#our $cf_default_priority = 1;
#our $cf_default_timeout = 300;
#our $cft_default_ringtimeout = 20;

#our $rollback_sql_export_filename_format = undef;
#our $rollback_sql_stmt_format = undef;

my $file_lock :shared = undef;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);

        $customer_export_filename_format = $data->{customer_export_filename} if exists $data->{customer_export_filename};
        get_export_filename($data->{customer_export_filename},$configfile);
              
        #$rollback_sql_export_filename_format = $data->{rollback_sql_export_filename_format} if exists $data->{rollback_sql_export_filename_format};
        #get_export_filename($data->{rollback_sql_export_filename_format},$configfile);
        #$rollback_sql_stmt_format = $data->{rollback_sql_stmt_format} if exists $data->{rollback_sql_stmt_format};

        $sqlite_db_file = $data->{sqlite_db_file} if exists $data->{sqlite_db_file};
        $csv_dir = $data->{csv_dir} if exists $data->{csv_dir};

        #$customer_import_filename = _get_import_filename($customer_import_filename,$data,'customer_import_filename');
        #$customer_import_multithreading = $data->{customer_import_multithreading} if exists $data->{customer_import_multithreading};
        #$customer_import_numofthreads = _get_numofthreads($cpucount,$data,'customer_import_numofthreads');
        #$customer_reseller_name = $data->{customer_reseller_name} if exists $data->{customer_reseller_name};
        #$customer_billing_profile_name = $data->{customer_billing_profile_name} if exists $data->{customer_billing_profile_name};
        #$customer_domain = $data->{customer_domain} if exists $data->{customer_domain};
        #$customer_contact_email_format = $data->{customer_contact_email_format} if exists $data->{customer_contact_email_format};
        #$subscriber_contact_email_format = $data->{subscriber_contact_email_format} if exists $data->{subscriber_contact_email_format};
        #$split_customers = $data->{split_customers} if exists $data->{split_customers};
        
        #$contract_timezone = $data->{customer_timezone} if exists $data->{customer_timezone};
        #if ($contract_timezone and not DateTime::TimeZone->is_valid_name($contract_timezone)) {
        #    configurationerror($configfile,"invalid customer_timezone '$contract_timezone'");
        #    $result = 0;
        #}

        #$subscriber_timezone = $data->{subscriber_timezone} if exists $data->{subscriber_timezone};
        #if ($subscriber_timezone and not DateTime::TimeZone->is_valid_name($subscriber_timezone)) {
        #    configurationerror($configfile,"invalid subscriber_timezone '$subscriber_timezone'");
        #    $result = 0;
        #}
        
        #$subscriber_profile_set_name = $data->{subscriber_profile_set_name} if exists $data->{subscriber_profile_set_name};
        #$subscriber_profile_name = $data->{subscriber_profile_name} if exists $data->{subscriber_profile_name};
        #if ($subscriber_profile_set_name and not $subscriber_profile_name
        #    or not $subscriber_profile_set_name and $subscriber_profile_name) {
        #    configurationerror($configfile,"both subscriber_profile_set_name and subscriber_profile_name required");
        #    $result = 0;
        #}
        #$webusername_format = $data->{webusername_format} if exists $data->{webusername_format};
        #$subscriber_externalid_format = $data->{subscriber_externalid_format} if exists $data->{subscriber_externalid_format};
        
        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $export_customers_multithreading = $data->{export_customers_multithreading} if exists $data->{export_customers_multithreading};
        $export_customers_numofthreads = _get_numofthreads($cpucount,$data,'export_customers_numofthreads');
        $export_customers_blocksize = $data->{export_customers_blocksize} if exists $data->{export_customers_blocksize};

        $tabular_yml = $data->{tabular_yml} if exists $data->{tabular_yml};
        $graph_yml = $data->{graph_yml} if exists $data->{graph_yml};
        $graph_fields_mode = $data->{graph_fields_mode} if exists $data->{graph_fields_mode};
        if (not $graph_fields_mode or not contains($graph_fields_mode,\@graph_fields_modes)) {
            configurationerror($configfile,'graph_fields_mode must be one of ' . join(', ', @graph_fields_modes));
            $result = 0;
        }
        $load_yml = $data->{load_yml} if exists $data->{load_yml};
        $tabular_single_row_txn = $data->{tabular_single_row_txn} if exists $data->{tabular_single_row_txn};
        $ignore_tabular_unique = $data->{ignore_tabular_unique} if exists $data->{ignore_tabular_unique};

        #$cf_default_priority = $data->{cf_default_priority} if exists $data->{cf_default_priority};
        #$cf_default_timeout = $data->{cf_default_timeout} if exists $data->{cf_default_timeout};
        #$cft_default_ringtimeout = $data->{cft_default_ringtimeout} if exists $data->{cft_default_ringtimeout};

        $csv_all_expected_fields = $data->{csv_all_expected_fields} if exists $data->{csv_all_expected_fields};
        
        $mr = $data->{schema_version};
        if (not defined $mr or not contains($mr,\@supported_mr)) {
            configurationerror($configfile,'schema_version must be one of ' . join(', ', @supported_mr));
            $result = 0;
        }
        
        return $result;
    }
    return 0;

}

sub run_dao_method {
    my $method_name = 'NGCP::BulkProcessor::Dao::' . $mr . '::' . shift;
    load_module($method_name);
    no strict 'refs';
    return $method_name->(@_);
}

sub get_dao_var {
    my $var_name = 'NGCP::BulkProcessor::Dao::' . $mr . '::' . shift;
    load_module($var_name);
    no strict 'refs';
    return @{$var_name} if wantarray;
    return ${$var_name};
}

sub _prepare_working_paths {

    my ($create) = @_;
    my $result = 1;
    my $path_result;

    ($path_result,$input_path) = create_path($working_path . 'input',$input_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;
    ($path_result,$output_path) = create_path($working_path . 'output',$output_path,$create,\&fileerror,getlogger(__PACKAGE__));
    $result &= $path_result;

    return $result;

}

sub _get_numofthreads {
    my ($default_value,$data,$key) = @_;
    my $numofthreads = $default_value;
    $numofthreads = $data->{$key} if exists $data->{$key};
    $numofthreads = $cpucount if $numofthreads > $cpucount;
    return $numofthreads;
}

sub get_export_filename {
    my ($filename_format,$configfile) = @_;
    my $export_filename;
    my $export_format;
    if ($filename_format) {
        $export_filename = sprintf($filename_format,timestampdigits(),threadid());
        unless ($export_filename =~ /^\//) {
            $export_filename = $output_path . $export_filename;
        }
        if (-e $export_filename and (unlink $export_filename) == 0) {
            filewarn('cannot remove ' . $export_filename . ': ' . $!,getlogger(__PACKAGE__));
            $export_filename = undef;
        }
        my ($name,$path,$suffix) = fileparse($export_filename,".json",".yml",".yaml",".xml",".php",".pl",".db",".csv");
        if ($suffix eq '.json') {
            $export_format = $NGCP::BulkProcessor::Serialization::format_json;
        } elsif ($suffix eq '.yml' or $suffix eq '.yaml') {
            $export_format = $NGCP::BulkProcessor::Serialization::format_yaml;
        } elsif ($suffix eq '.xml') {
            $export_format = $NGCP::BulkProcessor::Serialization::format_xml;
        } elsif ($suffix eq '.php') {
            $export_format = $NGCP::BulkProcessor::Serialization::format_php;
        } elsif ($suffix eq '.pl') {
            $export_format = $NGCP::BulkProcessor::Serialization::format_perl;
        } elsif ($suffix eq '.db') {
            $export_format = 'sqlite';
        } elsif ($suffix eq '.csv') {
            $export_format = 'csv';
        } else {
            configurationerror($configfile,"$filename_format: either .json/.yaml/.xml/.php/.pl or .db/.csv export file format required");
        }
    }
    return ($export_filename,$export_format);
}

sub write_export_file {

    my ($data,$export_filename,$export_format) = @_;
    if (defined $export_filename) {
        fileerror("invalid extension for output filename $export_filename",getlogger(__PACKAGE__))
            unless contains($export_format,\@NGCP::BulkProcessor::Serialization::formats);
        # "concatenated json" https://en.wikipedia.org/wiki/JSON_streaming
        my $str = '';
        if (ref $data eq 'ARRAY') {
            foreach my $obj (@$data) {
                #$str .= "\n" if length($str);
                $str .= NGCP::BulkProcessor::Serialization::serialize($obj,$export_format);
            }
        } else {
            $str = NGCP::BulkProcessor::Serialization::serialize($data,$export_format);
        }
        _write_file($str,$export_filename);
    }

}

sub write_sql_file {
    
    my ($data,$export_filename,$stmt_format) = @_;
    if (defined $export_filename and $stmt_format) {
        my $str = '';
        if (ref $data eq 'ARRAY') {
            foreach my $obj (@$data) {
                $str .= "\n" if length($str);
                if (ref $obj eq 'ARRAY') {
                    $str .= sprintf($stmt_format,@$obj);
                } else {
                    $str .= sprintf($stmt_format,$str);
                }
            }
        } else {
            $str = sprintf($stmt_format,$data);
        }
        $str .= "\n";
        _write_file($str,$export_filename);
    }
    
}

sub _write_file {

    my ($str,$export_filename) = @_;
    if (defined $export_filename) {
        lock $file_lock;
        open(my $fh, '>>', $export_filename) or fileerror('cannot open file ' . $export_filename . ': ' . $!,getlogger(__PACKAGE__));
        binmode($fh);
        print $fh $str;
        close $fh;
    }

}

sub update_tabular_fields {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $tabular_fields = $data;
        };
        if ($@ or 'ARRAY' ne ref $tabular_fields) {
            $tabular_fields //= [];
            configurationerror($configfile,'invalid tabular fields',getlogger(__PACKAGE__));
            $result = 0;
        }

        return $result;
    }
    return 0;

}

sub update_graph_fields {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $graph_fields = $data;
        };
        if ($@ or 'ARRAY' ne ref $graph_fields) {
            $graph_fields //= [];
            configurationerror($configfile,'invalid graph fields',getlogger(__PACKAGE__));
            $result = 0;
        }

        return $result;
    }
    return 0;

}

sub update_load_recursive {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;

        eval {
            $load_recursive = $data;
        };
        if ($@ or 'HASH' ne ref $load_recursive) {
            undef $load_recursive;
            configurationerror($configfile,'invalid load recursive def',getlogger(__PACKAGE__));
            $result = 0;
        }

        return $result;
    }
    return 0;

}

sub _get_import_filename {
    my ($old_value,$data,$key) = @_;
    my $import_filename = $old_value;
    $import_filename = $data->{$key} if exists $data->{$key};
    if (defined $import_filename and length($import_filename) > 0) {
        $import_filename = $input_path . $import_filename unless -e $import_filename;
    }
    return $import_filename;
}

sub check_dry {

    if ($dry) {
        scriptinfo('running in dry mode - NGCP databases will not be modified',getlogger(__PACKAGE__));
        return 1;
    } else {
        scriptinfo('NO DRY MODE - NGCP DATABASES WILL BE MODIFIED!',getlogger(__PACKAGE__));
        if (!$force) {
            if ('yes' eq lc(prompt("Type 'yes' to proceed: "))) {
                return 1;
            } else {
                return 0;
            }
        } else {
            scriptinfo('force option applied',getlogger(__PACKAGE__));
            return 1;
        }
    }

}

1;
