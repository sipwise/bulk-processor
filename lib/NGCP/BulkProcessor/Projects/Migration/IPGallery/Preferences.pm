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

1;
