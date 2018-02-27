package NGCP::BulkProcessor::Projects::Massive::Generator::Preferences;
use strict;

## no critic

no strict 'refs';

use threads::shared qw();
#use List::Util qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Settings qw(
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

use NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domains qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::domain_resellers qw();
use NGCP::BulkProcessor::Dao::Trunk::billing::voip_subscribers qw();

use NGCP::BulkProcessor::Projects::Migration::Teletek::Dao::import::Subscriber qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::Teletek::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

    clear_subscriber_preferences
    delete_subscriber_preference
    set_subscriber_preference
    get_subscriber_preference

    set_allowed_ips_preferences
    cleanup_aig_sequence_ids

);


sub cleanup_aig_sequence_ids {
    my ($context) = @_;
    eval {
        $context->{db}->db_begin();
        if (NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence::cleanup_ids($context->{db})) {
            _info($context,'voip_aig_sequence cleaned up');
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
        if ($skip_errors) {
            _warn($context,"database problem with voip_aig_sequence clean up: " . $err);
        } else {
            _error($context,"database problem with voip_aig_sequence clean up: " . $err);
        }
    }
}


sub set_allowed_ips_preferences {

    my ($context,$subscriber_id,$sip_username,$attribute,$allowed_ips) = @_;

    #my $subscriber_id = $context->{prov_subscriber}->{id} ;
    #my $attribute = $context->{attributes}->{allowed_ips_grp};

    my $allowed_ips_grp_attribute_preference = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::findby_subscriberid_attributeid(
        $context->{db},$subscriber_id,$attribute->{id})->[0];

    my ($allowed_ip_group_id,$allowed_ip_group_preferrence_id);

    if (defined $allowed_ips_grp_attribute_preference) {
        $allowed_ip_group_id = $allowed_ips_grp_attribute_preference->{value};
        $allowed_ip_group_preferrence_id = $allowed_ips_grp_attribute_preference->{id};
        NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups::delete_groupid($context->{db},$allowed_ip_group_id);
        _info($context,"allowed ips group for subscriber $sip_username exists, ipnets deleted",1);
    } else {
        $allowed_ip_group_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence::increment($context->{db});
        _info($context,"new allowed ips group id for subscriber $sip_username aquired",1);
    }

    my $allowed_ips_grp_ipnet_ids = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups::insert_rows($context->{db},$allowed_ip_group_id,$allowed_ips);
    _info($context,"ipnets for allowed ips group for subscriber $sip_username created",1);

    if (not defined $allowed_ips_grp_attribute_preference) {
        $allowed_ip_group_preferrence_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences::insert_row($context->{db},
                    attribute_id => $attribute->{id},
                    subscriber_id => $subscriber_id,
                    value => $allowed_ip_group_id,
                );
        _info($context,"new allowed ips group preference value for subscriber $sip_username added",1);
    }

    return ($allowed_ip_group_id,$allowed_ip_group_preferrence_id);

    #$context->{preferences}->{allowed_ips_grp} = { id => $allowed_ip_group_preferrence_id, $allowed_ip_group_id };

}

my %get_preference_sub_names = (
    voip_usr_preferences => 'findby_subscriberid_attributeid',
);
my %preference_id_cols = (
    voip_usr_preferences => 'subscriber_id',
);

sub clear_subscriber_preferences {
    my ($context,$subscriber_id,$attribute,$except_value) = @_;
    return _clear_preferences($context,'voip_usr_preferences',$subscriber_id,$attribute,$except_value);
}
sub delete_subscriber_preference {
    my ($context,$subscriber_id,$attribute,$value) = @_;
    return _delete_preference($context,'voip_usr_preferences',$subscriber_id,$attribute,$value);
}
sub set_subscriber_preference {
    my ($context,$subscriber_id,$attribute,$value) = @_;
    return _set_preference($context,'voip_usr_preferences',$subscriber_id,$attribute,$value);
}
sub get_subscriber_preference {
    my ($context,$subscriber_id,$attribute) = @_;
    return _get_preference($context,'voip_usr_preferences',$subscriber_id,$attribute);
}

sub _clear_preferences {
    my ($context,$pref_type,$id,$attribute,$except_value) = @_;

    return &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::delete_preferences'}($context->{db},
        $id, $attribute->{id}, defined $except_value ? { 'NOT IN' => $except_value } : undef);

}

sub _delete_preference {
    my ($context,$pref_type,$id,$attribute,$value) = @_;

    return &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::delete_preferences'}($context->{db},
        $id, $attribute->{id}, { 'IN' => $value } );

}

sub _set_preference {
    my ($context,$pref_type,$id,$attribute,$value) = @_;

    if ($attribute->{max_occur} == 1) {
        my $old_preferences = &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::' . $get_preference_sub_names{$pref_type}}($context->{db},
            $id,$attribute->{id});
        if (defined $value) {
            if ((scalar @$old_preferences) == 1) {
                &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::update_row'}($context->{db},{
                    id => $old_preferences->[0]->{id},
                    value => $value,
                });
                return $old_preferences->[0]->{id};
            } else {
                if ((scalar @$old_preferences) > 1) {
                    &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::delete_preferences'}($context->{db},
                        $id,$attribute->{id});
                }
                return &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::insert_row'}($context->{db},
                    attribute_id => $attribute->{id},
                    $preference_id_cols{$pref_type} => $id,
                    value => $value,
                );
            }
        } else {
            &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::delete_preferences'}($context->{db},
                $id,$attribute->{id});
            return undef;
        }
    } else {
        if (defined $value) {
            return &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::insert_row'}($context->{db},
                attribute_id => $attribute->{id},
                $preference_id_cols{$pref_type} => $id,
                value => $value,
            );
        } else {
            return undef;
        }
    }

}

sub _get_preference {
    my ($context,$pref_type,$id,$attribute) = @_;

    my $preferences = &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::' . $get_preference_sub_names{$pref_type}}($context->{db},
            $id,$attribute->{id});

    if ($attribute->{max_occur} == 1) {
        return $preferences->[0];
    } else {
        return $preferences;
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
