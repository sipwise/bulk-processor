package NGCP::BulkProcessor::Projects::ETL::CDR::ExportCDR;
use strict;

## no critic

use threads::shared qw();

use Tie::IxHash;

use NGCP::BulkProcessor::Serialization qw();
use Scalar::Util qw(blessed);
use MIME::Base64 qw(encode_base64);

use NGCP::BulkProcessor::Projects::ETL::CDR::Settings qw(
    $dry
    $skip_errors

    $export_cdr_multithreading
    $export_cdr_numofthreads
    $export_cdr_blocksize

    run_dao_method
    get_dao_var
    get_export_filename

    write_export_file
    $cdr_export_filename_format

    $tabular_fields
    $load_recursive
    $tabular_single_row_txn
    $ignore_tabular_unique
    $graph_fields
    $graph_fields_mode
);

use NGCP::BulkProcessor::Logging qw (
    getlogger
    processing_info
    processing_debug
);
use NGCP::BulkProcessor::LogError qw(
    rowprocessingerror
    rowprocessingwarn
    fileerror
);

use NGCP::BulkProcessor::Dao::Trunk::billing::contracts qw();

use NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular qw();

use NGCP::BulkProcessor::Projects::ETL::CDR::ProjectConnectorPool qw(
    get_sqlite_db
    destroy_all_dbs
    ping_all_dbs
);

use NGCP::BulkProcessor::Utils qw(create_uuid threadid timestamp stringtobool trim); #check_ipnet
#use NGCP::BulkProcessor::DSSorter qw(sort_by_configs);
#use NGCP::BulkProcessor::Table qw(get_rowhash);
use NGCP::BulkProcessor::Array qw(array_to_map);
use NGCP::BulkProcessor::DSPath qw();

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    export_cdr_graph
    export_cdr_tabular
);

sub _init_graph_field_map {
    my $context = shift;
    my %graph_field_map = ();
    my %graph_field_globs = ();
    tie(%graph_field_globs, 'Tie::IxHash');
    foreach my $graph_field (@$graph_fields) {
        my ($c,$a);
        if ('HASH' eq ref $graph_field) {
            $a = $graph_field->{path};
            $c = $graph_field;
        } else {
            $a = $graph_field;
            $c = 1;
        }
        #my @a = ();
        #my $b = '';
        #foreach my $c (split(/\./,$a)) {
        #    
        #    foreach my () {
        #        $b .= $_
        #        push()
        #    }
        #}
        if ($a =~ /\*/) {
            $a = quotemeta($a);
            $a =~ s/(\\\*)+/[^.]+/g;
            $a = '^' . $a . '$';
            $graph_field_globs{$a} = $c unless exists $graph_field_globs{$a};
        } else {
            $graph_field_map{$a} = $c unless exists $graph_field_map{$a};
        }
    }
    $context->{graph_field_map} = \%graph_field_map;
    $context->{graph_field_globs} = \%graph_field_globs;
}
    
sub export_cdr_graph {

    my $static_context = {

    };
    _init_graph_field_map($static_context);
    ($static_context->{export_filename},$static_context->{export_format}) = get_export_filename($cdr_export_filename_format);
    
    my $result = 1; #_copy_cdr_checks($static_context);

    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && run_dao_method('accounting::cdr::process_fromto',
        #source_dbs => $static_context->{source_dbs},
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            my @data = ();
            foreach my $record (@$records) {
                next unless _export_cdr_graph_init_context($context,$record);
                push(@data,_get_contract_graph($context));
            }
            write_export_file(\@data,$context->{export_filename},$context->{export_format});
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        destroy_reader_dbs_code => \&destroy_all_dbs,
        blocksize => $export_cdr_blocksize,
        multithreading => $export_cdr_multithreading,
        numofthreads => $export_cdr_numofthreads,
    ),$warning_count,);

}

sub export_cdr_tabular {

    my $result = NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::create_table(0);
    
    my $static_context = {
        upsert => _tabular_rows_reset_delta(),
    };
    
    destroy_all_dbs();
    my $warning_count :shared = 0;
    return ($result && run_dao_method('billing::contracts::process_records',
        static_context => $static_context,
        process_code => sub {
            my ($context,$records,$row_offset) = @_;
            ping_all_dbs();
            my @subscriber_rows = ();
            foreach my $record (@$records) {
                next unless _export_cdr_tabular_init_context($context,$record);
                push(@subscriber_rows, _get_subscriber_rows($context));
                
                if ($tabular_single_row_txn and (scalar @subscriber_rows) > 0) {
                    while (defined (my $subscriber_row = shift @subscriber_rows)) {
                        if ($skip_errors) {
                            eval { _insert_tabular_rows($context,[$subscriber_row]); };
                            _warn($context,$@) if $@;
                        } else {
                            _insert_tabular_rows($context,[$subscriber_row]);
                        }
                    }
                }
            }
            
            if (not $tabular_single_row_txn and (scalar @subscriber_rows) > 0) {
                if ($skip_errors) {
                    eval { insert_tabular_rows($context,\@subscriber_rows); };
                    _warn($context,$@) if $@;
                } else {
                    insert_tabular_rows($context,\@subscriber_rows);
                }
            }
            
            return 1;
        },
        init_process_context_code => sub {
            my ($context)= @_;
            $context->{db} = &get_sqlite_db();
            $context->{error_count} = 0;
            $context->{warning_count} = 0;
        },
        uninit_process_context_code => sub {
            my ($context)= @_;
            undef $context->{db};
            destroy_all_dbs();
            {
                lock $warning_count;
                $warning_count += $context->{warning_count};
            }
        },
        destroy_reader_dbs_code => \&destroy_all_dbs,
        blocksize => $export_cdr_blocksize,
        multithreading => $export_cdr_multithreading,
        numofthreads => $export_cdr_numofthreads,
    ),$warning_count,);

}

sub _tabular_rows_reset_delta {
    my $upsert = 0;
    if (NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::countby_delta() > 0) {
        processing_info(threadid(),'resetting delta of ' .
            NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::update_delta(undef,
            $NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::deleted_delta) .
            ' records',getlogger(__PACKAGE__));
        $upsert |= 1;
    }
    return $upsert;
}

sub _insert_tabular_rows {
    my ($context,$subscriber_rows) = @_;
    $context->{db}->db_do_begin(
        ($context->{upsert} ?
           NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::getupsertstatement()
         : NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::getinsertstatement($ignore_tabular_unique)),
    );
    eval {
        $context->{db}->db_do_rowblock($subscriber_rows);
        $context->{db}->db_finish();
    };
    my $err = $@;
    if ($err) {
        eval {
            $context->{db}->db_finish(1);
        };
        die($err);
    }

}

sub _export_cdr_graph_init_context {

    my ($context,$record) = @_;

    my $result = 1;

    return 0 unless _load_contract($context,$record);

    return $result;

}

sub _get_contract_graph {
    my ($context) = @_;
    
    my $dp = NGCP::BulkProcessor::DSPath->new($context->{contract}, {
        filter => sub {
            my $path = shift;
            if ('whitelist' eq $graph_fields_mode) {
                my $include;
                if (exists $context->{graph_field_map}->{$path}) {
                    $include = $context->{graph_field_map}->{$path};
                } else {
                    foreach my $glob (keys %{$context->{graph_field_globs}}) {
                        if ($path =~ /$glob/) {
                            $include = $context->{graph_field_globs}->{$glob};
                            last;
                        }
                    }
                }
                if ('HASH' eq ref $include) {
                    if (exists $include->{include}) {
                        return $include->{include};
                    }
                    return 1;
                } else {
                    return $include;
                }
            } elsif ('blacklist' eq $graph_fields_mode) {
                my $exclude;
                if (exists $context->{graph_field_map}->{$path}) {
                    $exclude = $context->{graph_field_map}->{$path};
                } else {
                    foreach my $glob (keys %{$context->{graph_field_globs}}) {
                        if ($path =~ /$glob/) {
                            $exclude = $context->{graph_field_globs}->{$glob};
                            last;
                        }
                    }
                }
                if ('HASH' eq ref $exclude) {
                    if (exists $exclude->{exclude}) {
                        return not $exclude->{exclude};
                    } elsif ($exclude->{transform}) {
                        return 1;
                    }
                    return 0;
                } else {
                    return not $exclude;
                }
            }
        },
        transform => sub {
            return shift;
            #    ($bill_subs->{provisioning_voip_subscriber}->{voip_usr_preferences}, my $as, my $vs) =
            #        array_to_map($bill_subs->{provisioning_voip_subscriber}->{voip_usr_preferences},
            #        sub { return shift->{attribute}; }, sub { my $p = shift; }, 'group' );
            #    if (my $prov_subscriber = $bill_subs->{provisioning_voip_subscriber}) {
            #        foreach my $voicemail_user (@{$prov_subscriber->{voicemail_users}}) {
            #            foreach my $voicemail (@{$voicemail_user->{voicemail_spool}}) {
            #                $voicemail->{recording} = encode_base64($voicemail->{recording},'');
            #            }
            #        }
            #    }              
        },
    });
    
    $dp->filter()->transform();
    
    return $context->{contract};
    
}

sub _export_cdr_tabular_init_context {

    my ($context,$record) = @_;

    my $result = 1;

    return 0 unless _load_contract($context,$record);
    
    if (defined $context->{contract}->{voip_subscribers}
        and not scalar @{$context->{contract}->{voip_subscribers}}) {
        _info($context,"contract ID $record->{id} has no subscribers, skipping",1);
        $result = 0;
    }

    return $result;

}

sub _get_subscriber_rows {

    my ($context) = @_;

    my @rows = ();
    foreach my $bill_subs (@{$context->{contract}->{voip_subscribers}}) {
        my @row = ();
        $bill_subs->{contract} = NGCP::BulkProcessor::Dao::Trunk::billing::contracts->new($context->{contract}); #no circular ref
        ($bill_subs->{provisioning_voip_subscriber}->{voip_usr_preferences}, my $as, my $vs) =
            array_to_map($bill_subs->{provisioning_voip_subscriber}->{voip_usr_preferences},
            sub { return shift->{_attribute}; }, sub { my $p = shift; }, 'group' );
        if (my $prov_subscriber = $bill_subs->{provisioning_voip_subscriber}) {
            foreach my $voicemail_user (@{$prov_subscriber->{voicemail_users}}) {
                foreach my $voicemail (@{$voicemail_user->{voicemail_spool}}) {
                    $voicemail->{recording} = encode_base64($voicemail->{recording},'');
                }
            }
        }               
        my $dp = NGCP::BulkProcessor::DSPath->new($bill_subs, {
            retrieve_key_from_non_hash => sub {},
            key_does_not_exist => sub {},
            index_does_not_exist => sub {},
        });
        foreach my $tabular_field (@$tabular_fields) {
            my $a;
            my $sep = ',';
            my $transform;
            if ('HASH' eq ref $tabular_field) {
                $a = $tabular_field->{path};
                $sep = $tabular_field->{sep};
                $transform = $tabular_field->{transform};
            } else {
                $a = $tabular_field;
            }
            #eval {'' . ($dp->get('.' . $a) // '');}; if($@){
            #    my $x=5;
            #}
            my $v = $dp->get('.' . $a);
            if ('CODE' eq ref $transform) {
                my $closure = _closure($transform,_get_closure_context($context));
                $v = $closure->($v,$bill_subs);
            }
            if ('ARRAY' eq ref $v) {
                if ('HASH' eq ref $v->[0]
                    or (blessed($v->[0]) and $v->[0]->isa('NGCP::BulkProcessor::SqlRecord'))) {
                    $v = join($sep, sort map { $_->{$tabular_field->{field}}; } @$v);
                } else {
                    $v = join($sep, sort @$v);
                }
            } else {
                $v = '' . ($v // '');
            }
            push(@row,$v);
        }
        push(@row,$bill_subs->{uuid}) unless grep { 'uuid' eq $_; } @{NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::get_fieldnames()};
        if ($context->{upsert}) {
            push(@row,$bill_subs->{uuid});
        } else {
            push(@row,$NGCP::BulkProcessor::Projects::ETL::CDR::Dao::Tabular::added_delta);
        }

        push(@rows,\@row);
    }

    return @rows;

}

sub _load_contract {
    
    my ($context,$record) = @_;
    $context->{contract} = run_dao_method('billing::contracts::findby_id', $record->{id}, { %$load_recursive,
        #'contracts.voip_subscribers.domain' => 1,
        _context => _get_closure_context($context),
    });
    
    return 1 if $context->{contract};
    return 0;
    
}

sub _get_closure_context {
    my $context = shift;
    return {
        _info => \&_info,
        _error => \&_error,
        _debug => \&_debug,
        _warn => \&_warn,
        context => $context,
    };
}

sub _closure {
    my ($sub,$context) = @_;
    return sub {
        foreach my $key (keys %$context) {
            no strict "refs";  ## no critic (ProhibitNoStrict)
            *{"main::$key"} = $context->{$key} if 'CODE' eq ref $context->{$key};
        }
        return $sub->(@_,$context);
    };
}

sub _error {

    my ($context,$message) = @_;
    $context->{error_count} = $context->{error_count} + 1;
    rowprocessingerror($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _warn {

    my ($context,$message) = @_;
    $context->{warning_count} = $context->{warning_count} + 1;
    rowprocessingwarn($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

sub _info {

    my ($context,$message,$debug) = @_;
    if ($debug) {
        processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    } else {
        processing_info($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));
    }
}

sub _debug {

    my ($context,$message,$debug) = @_;
    processing_debug($context->{tid} // threadid(),$message,getlogger(__PACKAGE__));

}

1;
