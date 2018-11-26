package NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Api;
use strict;

## no critic

use threads::shared qw();
#use List::Util qw();
#use DateTime qw();

#use NGCP::BulkProcessor::Globals qw(
#    $system_abbreviation
#);

use NGCP::BulkProcessor::Projects::ETL::SetProfilePackage::Settings qw(
    $dry
    $skip_errors

    $mappings

    $set_profile_package_multithreading
    $set_profile_package_numofthreads

);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
);

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Array qw(array_to_map);

use NGCP::BulkProcessor::RestRequests::Trunk::Customers qw();
use NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles qw();
use NGCP::BulkProcessor::RestRequests::Trunk::ProfilePackages qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    set_profile_package
);

sub set_profile_package {

    my $static_context = {};
    my $result = _set_profile_package_init_context($static_context);

    my $warning_count :shared = 0;
    return ($result && NGCP::BulkProcessor::RestRequests::Trunk::Customers::process_items(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            foreach my $contract (@$records) {
                next unless _set_profile_package_reset_context($context,$contract);
                _update_contract($context);
            }
            return 1;
        },
        init_process_context_code => sub {
            my ($context) = @_;
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
        },
        uninit_process_context_code => sub {
            my ($context) = @_;
            #destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        load_recursive => 1,
        multithreading => $set_profile_package_multithreading,
        numofthreads => $set_profile_package_numofthreads,
    ),$warning_count);
}

sub _set_profile_package_reset_context {

    my ($context,$contract) = @_;

    my $result = 0;

    $context->{contract} = $contract;

    $context->{package} = undef;
    if (exists $context->{mappings}->{$contract->{billing_profile_id}}
        and (
            not defined $contract->{profile_package_id}
            or $contract->{profile_package_id} != $context->{mappings}->{$contract->{billing_profile_id}}
        )) {
        $result = 1;
        $context->{package} = $context->{profile_package_map}->{$context->{mappings}->{$contract->{billing_profile_id}}};
    }

    return $result;

}

sub _set_profile_package_init_context {
    my ($context) = @_;

    my $result = 1;
    my @billing_profiles = ();
    foreach my $handle (keys %$mappings) {
        rowprocessingerror(threadid(),"no profile package for billing profile '$handle'",getlogger(__PACKAGE__)) unless defined $mappings->{$handle};
        my $billing_profile;
        eval {
            $billing_profile = NGCP::BulkProcessor::RestRequests::Trunk::BillingProfiles::findby_handle(
                $handle,#$reseller_id
            )->[0];
        };
        if ($@ or not $billing_profile) {
            rowprocessingerror(threadid(),"cannot find billing profile '$handle'",getlogger(__PACKAGE__));
            $result = 0; #even in skip-error mode..
        } else {
            push(@billing_profiles,$billing_profile);
        }
    }
    ($context->{billing_profile_map},my $ids,my $vals) = array_to_map(
        \@billing_profiles, sub { return shift->{id}; }, sub { return shift; }, 'first' );
    my %profile_packages = ();
    foreach my $name (values %$mappings) {
        next if exists $profile_packages{$name};
        my $profile_package;
        eval {
            $profile_package = NGCP::BulkProcessor::RestRequests::Trunk::ProfilePackages::findby_name(
                $name,#$reseller_id
            )->[0];
        };
        if ($@ or not $profile_package) {
            rowprocessingerror(threadid(),"cannot find profile package '$name'",getlogger(__PACKAGE__));
            $result = 0; #even in skip-error mode..
        } else {
            $profile_packages{$name} = $profile_package;
        }
    }
    ($context->{profile_package_map}, $ids, $vals) = array_to_map(
        [ values %profile_packages ], sub { return shift->{id}; }, sub { return shift; }, 'first' );
    $context->{mappings} = {
        map { $_->{id} => $profile_packages{$mappings->{$_->{handle}}}->{id}; } @billing_profiles
    };

    return $result;
}

sub _update_contract {
    my ($context) = @_;

    my $result = 0;
    my $contract_path = NGCP::BulkProcessor::RestRequests::Trunk::Customers::get_item_path(
        $context->{contract}->{id});
    eval {
        my $customer;
        if ($dry) {
            $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::get_item(
                $context->{contract}->{id}
            );
        } else {
            $customer = NGCP::BulkProcessor::RestRequests::Trunk::Customers::update_item(
                $context->{contract}->{id},
                {
                    billing_profile_definition => 'package',
                    profile_package_id => $context->{package}->{id},
                },
            );
        }
        $result = (defined $customer ? 1 : 0);
    };
    if ($@ or not $result) {
        if ($skip_errors) {
            _warn($context,'could not ' . ($dry ? 'fetch' : 'update') . ' ' . $contract_path);
        } else {
            _error($context,'could not ' . ($dry ? 'fetch' : 'update') . ' ' . $contract_path);
        }
    } else {
        _info($context,$contract_path . ($dry ? ' fetched' : ' updated'));
    }
    return $result;

}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid},$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid},$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid},$message,getlogger(__PACKAGE__));
    }
}

1;
