package NGCP::BulkProcessor::SqlRecord;
use strict;

## no critic

use NGCP::BulkProcessor::Table qw(get_rowhash);

use NGCP::BulkProcessor::SqlProcessor qw(init_record);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw();

sub new {

    my $class = shift;
    my $self = bless {}, $class;
    return init_record($self,$class,@_);

}

sub gethash {
    my $self = shift;
    my @fieldvalues = ();
    foreach my $field (sort keys %$self) { #http://www.perlmonks.org/?node_id=997682
        my $value = $self->{$field};
        if (ref $value eq '') {
            push(@fieldvalues,$value);
        }
    }
    return get_rowhash(\@fieldvalues);
}

1;
