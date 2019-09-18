package NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Settings;
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
use NGCP::BulkProcessor::Utils qw(prompt timestampdigits);
#format_number check_ipnet
use NGCP::BulkProcessor::Array qw(contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    update_settings
    run_dao_method

    check_dry

    $output_path
    $report_filename

    $defaultsettings
    $defaultconfig

    $dry
    $skip_errors
    $force

    $source_accounting_databasename
    $source_accounting_username
    $source_accounting_password
    $source_accounting_host
    $source_accounting_port

    $source_billing_databasename
    $source_billing_username
    $source_billing_password
    $source_billing_host
    $source_billing_port

    $source_provisioning_databasename
    $source_provisioning_username
    $source_provisioning_password
    $source_provisioning_host
    $source_provisioning_port

    $source_kamailio_databasename
    $source_kamailio_username
    $source_kamailio_password
    $source_kamailio_host
    $source_kamailio_port

    $source_rowblock_transactional

    $copy_contract_multithreading
    $copy_contract_numofthreads
    $copy_contract_blocksize

    $copy_billing_fees_multithreading
    $copy_billing_fees_numofthreads

);

our $defaultconfig = 'config.cfg';
our $defaultsettings = 'settings.cfg';

our $output_path = $working_path . 'output/';
our $report_filename = undef;

our $force = 0;
our $dry = 0;
our $skip_errors = 0;

my $mr = 'Trunk';
my @supported_mr = ('Trunk','mr341','mr553');

our	$source_accounting_databasename = 'accounting';
our $source_accounting_username = 'root';
our	$source_accounting_password = '';
our $source_accounting_host = '127.0.0.1';
our $source_accounting_port = '3306';

our	$source_billing_databasename = 'billing';
our $source_billing_username = 'root';
our	$source_billing_password = '';
our $source_billing_host = '127.0.0.1';
our $source_billing_port = '3306';

our	$source_provisioning_databasename = 'provisioning';
our $source_provisioning_username = 'root';
our	$source_provisioning_password = '';
our $source_provisioning_host = '127.0.0.1';
our $source_provisioning_port = '3306';

our	$source_kamailio_databasename = 'kamailio';
our $source_kamailio_username = 'root';
our	$source_kamailio_password = '';
our $source_kamailio_host = '127.0.0.1';
our $source_kamailio_port = '3306';

our $source_rowblock_transactional = undef; #connector default

our $copy_contract_multithreading = $enablemultithreading;
our $copy_contract_numofthreads = $cpucount;
our $copy_contract_blocksize = 100;

our $copy_billing_fees_multithreading = $enablemultithreading;
our $copy_billing_fees_numofthreads = $cpucount;

sub update_settings {

    my ($data,$configfile) = @_;

    if (defined $data) {

        my $result = 1;
        my $regexp_result;

        #&$configurationinfocode("testinfomessage",$configlogger);

        $result &= _prepare_working_paths(1);
        if ($data->{report_filename}) {
            $report_filename = $output_path . sprintf('/' . $data->{report_filename},timestampdigits());
            if (-e $report_filename and (unlink $report_filename) == 0) {
                filewarn('cannot remove ' . $report_filename . ': ' . $!,getlogger(__PACKAGE__));
                $report_filename = undef;
            }
        } else {
            $report_filename = undef;
        }

        $dry = $data->{dry} if exists $data->{dry};
        $skip_errors = $data->{skip_errors} if exists $data->{skip_errors};

        $source_accounting_host = $data->{source_accounting_host} if exists $data->{source_accounting_host};
        $source_accounting_port = $data->{source_accounting_port} if exists $data->{source_accounting_port};
        $source_accounting_databasename = $data->{source_accounting_databasename} if exists $data->{source_accounting_databasename};
        $source_accounting_username = $data->{source_accounting_username} if exists $data->{source_accounting_username};
        $source_accounting_password = $data->{source_accounting_password} if exists $data->{source_accounting_password};

        $source_billing_host = $data->{source_billing_host} if exists $data->{source_billing_host};
        $source_billing_port = $data->{source_billing_port} if exists $data->{source_billing_port};
        $source_billing_databasename = $data->{source_billing_databasename} if exists $data->{source_billing_databasename};
        $source_billing_username = $data->{source_billing_username} if exists $data->{source_billing_username};
        $source_billing_password = $data->{source_billing_password} if exists $data->{source_billing_password};

        $source_provisioning_host = $data->{source_provisioning_host} if exists $data->{source_provisioning_host};
        $source_provisioning_port = $data->{source_provisioning_port} if exists $data->{source_provisioning_port};
        $source_provisioning_databasename = $data->{source_provisioning_databasename} if exists $data->{source_provisioning_databasename};
        $source_provisioning_username = $data->{source_provisioning_username} if exists $data->{source_provisioning_username};
        $source_provisioning_password = $data->{source_provisioning_password} if exists $data->{source_provisioning_password};

        $source_kamailio_host = $data->{source_kamailio_host} if exists $data->{source_kamailio_host};
        $source_kamailio_port = $data->{source_kamailio_port} if exists $data->{source_kamailio_port};
        $source_kamailio_databasename = $data->{source_kamailio_databasename} if exists $data->{source_kamailio_databasename};
        $source_kamailio_username = $data->{source_kamailio_username} if exists $data->{source_kamailio_username};
        $source_kamailio_password = $data->{source_kamailio_password} if exists $data->{source_kamailio_password};


        $source_rowblock_transactional = $data->{source_rowblock_transactional} if exists $data->{source_rowblock_transactional};

        $copy_contract_multithreading = $data->{copy_contract_multithreading} if exists $data->{copy_contract_multithreading};
        $copy_contract_numofthreads = _get_numofthreads($cpucount,$data,'copy_contract_numofthreads');
        $copy_contract_blocksize = $data->{copy_contract_blocksize} if exists $data->{copy_contract_blocksize};

        $copy_billing_fees_multithreading = $data->{copy_billing_fees_multithreading} if exists $data->{copy_billing_fees_multithreading};
        $copy_billing_fees_numofthreads = _get_numofthreads($cpucount,$data,'copy_billing_fees_numofthreads');

        $mr = $data->{version};
        if (not defined $mr or not contains($mr,\@supported_mr)) {
            configurationerror($configfile,'version must be one of ' . join(', ', @supported_mr));
            $result = 0;
        }

        return $result;

    }
    return 0;

}

sub run_dao_method {
    my $method_name = 'NGCP::BulkProcessor::Dao::' . $mr . '::' . shift;
    no strict 'refs';
    return $method_name->(@_);
}

sub _prepare_working_paths {

    my ($create) = @_;
    my $result = 1;
    my $path_result;

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
