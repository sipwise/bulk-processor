package NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Preferences;
use strict;

## no critic

no strict 'refs';

use threads::shared qw();
#use List::Util qw();

use NGCP::BulkProcessor::Projects::Migration::CommunicationConcept::Settings qw(
    $dry
    $skip_errors

    run_dao_method
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

#use NGCP::BulkProcessor::Projects::Migration::UPCAT::Dao::import::MtaSubscriber qw();

use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_subscribers qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_usr_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_dom_preferences qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence qw();
use NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups qw();

use NGCP::BulkProcessor::ConnectorPool qw(
    get_xa_db
);

use NGCP::BulkProcessor::Projects::Migration::UPCAT::ProjectConnectorPool qw(
    destroy_all_dbs
);

use NGCP::BulkProcessor::Utils qw(threadid);
use NGCP::BulkProcessor::Array qw(array_to_map contains);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(

    clear_subscriber_preferences
    delete_subscriber_preference
    set_subscriber_preference
    get_subscriber_preference

    cleanup_aig_sequence_ids
    create_usr_preferences
    create_dom_preferences

    check_replaced_prefs

    map_preferences

);

my %get_preference_sub_names = (
    voip_usr_preferences => 'findby_subscriberid_attributeid',
    voip_dom_preferences => 'findby_domainid_attributeid',
);
my %preference_id_cols = (
    voip_usr_preferences => 'subscriber_id',
    voip_dom_preferences => 'domain_id',
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

my %obsolete_prefs = (
    srtp_transcoding => {
        add => [ 'transport_protocol' ],
        remove => [ 'rtcp_feedback' ],
        transform_values => sub {
            my ($value,$map) = @_;
            my $new_value;
            if ($value eq 'force_rtp' and $map->{rtcp_feedback}->[0] eq 'force_avp') {
                $new_value = 'RTP/AVP';
            } elsif ($value eq 'force_rtp' and $map->{rtcp_feedback}->[0] eq 'force_avpf') {
                $new_value = 'RTP/AVPF';
            } elsif ($value eq 'force_srtp' and $map->{rtcp_feedback}->[0] eq 'force_avp') {
                $new_value = 'RTP/SAVP';
            } elsif ($value eq 'force_srtp' and $map->{rtcp_feedback}->[0] eq 'force_avpf') {
                $new_value = 'RTP/SAVPF';
            } elsif ($value eq 'transparent' or $map->{rtcp_feedback}->[0] eq 'transparent') {
                $new_value = 'transparent';
            }
            return ($new_value);
        },
    },
    #rtcp_feedback => {
    #    add => [ 'transport_protocol' ],
    #    remove => [ 'srtp_transcoding' ],
    #    transform_values => sub {
    #        my ($value,$map) = @_;
    #        my $new_value;
    #        if ($value eq 'force_rtp' and $map->{rtcp_feedback}->[0] eq 'force_avp') {
    #            $new_value = 'RTP/AVP';
    #        } elsif ($value eq 'force_rtp' and $map->{rtcp_feedback}->[0] eq 'force_avpf') {
    #            $new_value = 'RTP/AVPF';
    #        } elsif ($value eq 'force_srtp' and $map->{rtcp_feedback}->[0] eq 'force_avp') {
    #            $new_value = 'RTP/SAVP';
    #        } elsif ($value eq 'force_srtp' and $map->{rtcp_feedback}->[0] eq 'force_avpf') {
    #            $new_value = 'RTP/SAVPF';
    #        } elsif ($value eq 'transparent' or $map->{rtcp_feedback}->[0] eq 'transparent') {
    #            $new_value = 'transparent';
    #        }
    #        return ('transport_protocol',$new_value,'srtp_transcoding');
    #    },
    #},
);
my %remove_check = map { $_ => 1; } map { @{$obsolete_prefs{$_}->{remove}}; } keys %obsolete_prefs;

sub check_replaced_prefs {
    my $attribute = shift;
    return (1,[]) if exists $remove_check{$attribute};
    return (1,$obsolete_prefs{$attribute}->{add}) if exists $obsolete_prefs{$attribute};
    return (0,[]);
}

sub _transform_obsolete_prefs {

    my ($prefs) = @_;

    my @transformed = ();
    my $map;
    foreach my $pref (@$prefs) {
        if (exists $obsolete_prefs{$pref->{attribute}}) {
            ($map, my $attributes, my $ps) = array_to_map($prefs, sub {
                return shift->{attribute};
            }, sub {
                return shift;
            }, 'group') unless $map;
            my $replacement = $obsolete_prefs{$pref->{attribute}};
            my @new_values = $replacement->{transform_values}->($pref->{value},$map);
            my $i = 0;
            foreach my $add (@{$replacement->{add}}) {
                push(@transformed,{
                    attribute => $add,
                    value => $new_values[$i],
                });
                $i++;
            }
        } elsif (not exists $remove_check{$pref->{attribute}}) {
            push(@transformed,$pref);
        }
    }
    return \@transformed;

}


sub create_usr_preferences {
    my ($context, $prov_subscriber, $ps) = @_;
    return _create_preferences($context,'voip_usr_preferences','usr_pref',$ps,{
        account_id => $prov_subscriber->{account_id},
        id => $prov_subscriber->{id},
    });
}

sub create_dom_preferences {
    my ($context, $prov_domain, $ps) = @_;
    return _create_preferences($context,'voip_dom_preferences','dom_pref',$ps,{
        id => $prov_domain->{id},
    });
}

sub _create_preferences {

    my ($context, $pref_type, $pref_flag, $ps, $vals) = @_;

    my $result = 1;
    (my $preferrences_map, my $attributes, $ps) = array_to_map(_transform_obsolete_prefs($ps), sub {
            return shift->{attribute};
        }, sub {
            return shift;
        }, 'group');
    my $rwrs;
    foreach my $a (sort keys %$preferrences_map) {
        my $attribute = $context->{attribute_map}->{$a};
        #unless ($attribute) {
        #    die();
        #}
        foreach my $v (@{$preferrences_map->{$a}}) {
            my $value = { %$v };
            delete $value->{id};
            $value->{attribute_id} = $attribute->{id};
            delete $value->{attribute};
            #&$set_entityid_code($value);
            $value->{$preference_id_cols{$pref_type}} = $vals->{id};
            if ($a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ALLOWED_IPS_GRP_ATTRIBUTE) {
                my $allowed_ips = delete $value->{allowed_ip_groups};
                my $allowed_ip_group_id = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_aig_sequence::increment($context->{db});
                my $allowed_ips_grp_ipnet_ids = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_allowed_ip_groups::insert_rows($context->{db},
                    $allowed_ip_group_id,$allowed_ips);
                $value->{value} = $allowed_ip_group_id;
                _info($context,"ipnets for allowed ips group created",1);
                $context->{cleanup_aig_sequence} |= 1;
            } elsif ($a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ACCOUNT_ID_ATTRIBUTE) {
                $value->{value} = $vals->{account_id};
            } elsif ($a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::NCOS_ID_ATTRIBUTE
                     or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_NCOS_ID_ATTRIBUTE
                     or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::ADM_CF_NCOS_ID_ATTRIBUTE) {
                if (exists $context->{ncos_level_id_map}->{$value->{value}}) {
                    $value->{value} = $context->{ncos_level_id_map}->{$value->{value}};
                } else {
                    _warn($context,"cannot find $a $value->{value}, skipping");
                    $result = 0;
                    next;
                }
            } elsif (exists $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::DPID_ATTRIBUTES{$a}) {
                if (not defined $rwrs) {
                    if (exists $context->{dpid_rwrs_map}->{$value->{value}}) {
                        $rwrs = $context->{dpid_rwrs_map}->{$value->{value}};
                        foreach my $dpid_attribute (keys %NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::DPID_ATTRIBUTES) {
                            $value->{attribute_id} = $context->{attribute_map}->{$dpid_attribute}->{id};
                            $value->{value} = $rwrs->{$NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::DPID_ATTRIBUTES{$dpid_attribute}};
                            &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::insert_row'}($context->{db},
                                $value,
                            );
                        }
                    } else {
                        _warn($context,"cannot find rewrite rule set for $a $value->{value}");
                        $result = 0;
                    }
                #} else {
                }
                next;
            } elsif (contains($a,\@NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CF_ATTRIBUTES)) {
                #todo
                next;
            } elsif ($a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::SOUND_SET_ATTRIBUTE
                    or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CONTRACT_SOUND_SET_ATTRIBUTE) {
                if (exists $context->{sound_set_id_map}->{$value->{value}}) {
                    $value->{value} = $context->{sound_set_id_map}->{$value->{value}};
                } else {
                    _warn($context,"cannot find $a $value->{value}, skipping");
                    $result = 0;
                    next;
                }
            } elsif ($a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::CDR_EXPORT_SCLIDUI_RWRS_ID_ATTRIBUTE
                     or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::MAN_ALLOWED_IPS_GRP_ATTRIBUTE
                     or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::EMERGENCY_MAPPING_CONTAINER_ID_ATTRIBUTE
                     or $a eq $NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_preferences::HEADER_RULE_SET_ATTRIBUTE) {
                _error($context,"migrating $a preference not implemented");
            }
            if ($attribute->has_enum_default($pref_flag)) {
                _set_preference($context,$pref_type,$vals->{id},$attribute,$value->{value});
            } else {
                &{'NGCP::BulkProcessor::Dao::Trunk::provisioning::' . $pref_type . '::insert_row'}($context->{db},
                    $value,
                );
            }
        }
    }
    return $result;
}


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

sub map_preferences {

    my $context = shift;

    my $result = 1;

    $context->{ncos_level_id_map} = {};
    $context->{dpid_rwrs_map} = {};
    $context->{sound_set_id_map} = {};

    eval {
        foreach my $old_reseller (@{run_dao_method('billing::resellers::source_findall',$context->{source_dbs})}) {
            my $new_reseller = NGCP::BulkProcessor::Dao::Trunk::billing::resellers::findby_name($old_reseller->{name});
            die("reseller '$old_reseller->{name}' not found") unless $new_reseller;
            foreach my $old_ncos_level (@{$old_reseller->{ncos_levels}}) {
                my $new_ncos_level = NGCP::BulkProcessor::Dao::Trunk::billing::ncos_levels::findby_resellerid_level($new_reseller->{id},
                    $old_ncos_level->{level},
                );
                die("ncos level '$old_ncos_level->{level}' not found") unless $new_ncos_level;
                $context->{ncos_level_id_map}->{$old_ncos_level->{id}} = $new_ncos_level->{id};
            }
            foreach my $old_rwrs (@{$old_reseller->{rewrite_rule_sets}}) {
                my $new_rwrs = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_rewrite_rule_sets::findby_name(#$new_reseller->{id},
                    $old_rwrs->{name},
                );
                die("rewrite rule set '$old_reseller->{name}' not found") unless $new_rwrs;
                @{$context->{dpid_rwrs_map}}{map { $old_rwrs->{$_}; } @NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_rewrite_rule_sets::DPID_FIELDS} =
                    map { $new_rwrs; } @NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_rewrite_rule_sets::DPID_FIELDS;
            }
            foreach my $old_sound_set (@{$old_reseller->{sound_sets}}) {
                my $new_sound_set = NGCP::BulkProcessor::Dao::Trunk::provisioning::voip_sound_sets::findby_resellerid_name($new_reseller->{id},
                    $old_sound_set->{name},
                );
                die("sound set '$old_sound_set->{name}' not found") unless $new_sound_set;
                $context->{sound_set_id_map}->{$old_sound_set->{id}} = $new_sound_set->{id};
            }
        }
    };
    if ($@) {
        _error($context,$@);
        $result = 0; #even in skip-error mode..
    } else {
        _info($context,(scalar keys %{$context->{ncos_level_id_map}}) . " ncos levels mapped");
        _info($context,(scalar keys %{$context->{dpid_rwrs_map}}) . " dpids mapped");
        _info($context,(scalar keys %{$context->{sound_set_id_map}}) . " sound sets mapped");
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
