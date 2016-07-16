package NGCP::BulkProcessor::Projects::Migration::IPGallery::Preferences;
use strict;

## no critic

use NGCP::BulkProcessor::Projects::Migration::IPGallery::Settings qw(
    $dry
    $skip_errors
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

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::IPGallery::ProjectConnectorPool qw(
    destroy_all_dbs
);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    clear_preferences
    set_preference
);


sub write_barring_profiles {

    my $static_context = {};
    my $result;
    #my $result = _write_barring_profiles_checks($static_context);

    destroy_all_dbs();
    return $result && NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::process_records(
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            my $rownum = $row_offset;
            foreach my $imported_subscriber (@$records) {
                $rownum++;
                next unless _write_barring_profile($context,$imported_subscriber,$rownum);
            }

            #return 0;
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_xa_db();

            # below is not mandatory..
            _check_insert_tables();
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
        },
        load_recursive => 0,
        #multithreading => $provision_subscriber_multithreading,
        #numofthreads => $provision_subscriber_numofthreads,
    );
}

sub _check_insert_tables {

    NGCP::BulkProcessor::Dao::Trunk::billing::contacts::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::contracts::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::billing_mappings::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::contract_balances::check_table();
    NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::check_table();
    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dbaliases::check_table();
    NGCP::BulkProcessor::Dao::Trunk::kamailio::voicemail_users::check_table();

}

sub _write_barring_profile {
    my ($context,$imported_subscriber,$rownum) = @_;

    return 0 unless _reset_context($context,$imported_subscriber,$rownum);

    eval {
        $context->{db}->db_begin();
        #rowprocessingwarn($context->{tid},'AutoCommit is on' ,getlogger(__PACKAGE__)) if $context->{db}->{drh}->{AutoCommit};

        my $existing_billing_voip_subscribers = NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::findby_domainid_username_states($context->{db},
            $context->{billing_domain}->{id},$context->{username},{ 'NOT IN' => $NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers::TERMINATED_STATE});
        if ((scalar @$existing_billing_voip_subscribers) == 1) {
            my $existing_billing_voip_subscriber = $existing_billing_voip_subscribers->[0];
            if ($imported_subscriber->{delta} eq
                $NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::deleted_delta) {

                processing_info($context->{tid},"($context->{rownum}) " . 'subscriber ' . $context->{cli} . ' is deleted, but active subscriber found',getlogger(__PACKAGE__));

            } else {
                #todo
            }
        } else {
            rowprocessingwarn($context->{tid},"($context->{rownum}) " . 'multiple (' . (scalar @$existing_billing_voip_subscribers) . ') existing billing subscribers with username ' . $context->{username} . ' found, skipping' ,getlogger(__PACKAGE__));
        }

        if ($dry) {
            $context->{db}->db_rollback(0);
        } else {
            $context->{db}->db_commit();
        }

    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_rollback(1);
        };
        die($err) if !$skip_errors;
    }

    return 1;

}

sub _write_barring_profiles_checks {
    my ($context) = @_;

    my $result = 1;
    my $optioncount = 0;
    eval {
        $optioncount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::FeatureOption::countby_subscribernumber_option();
    };
    if ($@ or $optioncount == 0) {
        rowprocessingerror(threadid(),'please import subscriber features first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }
    my $subscribercount = 0;
    eval {
        $subscribercount = NGCP::BulkProcessor::Projects::Migration::IPGallery::Dao::import::Subscriber::countby_subscribernumber();
    };
    if ($@ or $subscribercount == 0) {
        rowprocessingerror(threadid(),'please import subscribers first',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    eval {
        $context->{adm_ncos_attribute} = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::findby_attribute(
            $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ATTRIBUTE);
    };
    if ($@ or not defined $context->{adm_ncos_attribute}) {
        rowprocessingerror(threadid(),'cannot find adm_ncos attribute',getlogger(__PACKAGE__));
        $result = 0; #even in skip-error mode..
    }

    return $result;
}



























sub clear_preferences {
    my ($context,$subscriber_id,$attribute,$except_value) = @_;

    return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
        $subscriber_id,$attribute->{id},defined $except_value ? { 'NOT IN' => $except_value } : undef);

}

sub set_preference {
    my ($context,$subscriber_id,$attribute,$value) = @_;

    my $old_preferences = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::findby_subscriberid_attributeid($context->{db},
            $subscriber_id,$attribute->{id});

    if ($attribute->{max_occur} == 1) {
        if (defined $value) {
            if ((scalar @$old_preferences) == 1) {
                NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::update_row($context->{db},{
                    id => $old_preferences->[0]->{id},
                    value => $value,
                });
                return $old_preferences->[0]->{id};
            } else {
                if ((scalar @$old_preferences) > 1) {
                    NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
                        $subscriber_id,$attribute->{id});
                }
                return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::insert_row($context->{db},
                    attribute_id => $attribute->{id},
                    subscriber_id => $subscriber_id,
                    value => $value,
                );
            }
        } else {
            NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::delete_preferences($context->{db},
                $subscriber_id,$attribute->{id});
            return undef;
        }
    } else {
        if (defined $value) {
            return NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::insert_row($context->{db},
                attribute_id => $attribute->{id},
                subscriber_id => $subscriber_id,
                value => $value,
            );
        } else {
            return undef;
        }
    }

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
