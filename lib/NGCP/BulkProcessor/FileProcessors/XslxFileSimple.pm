package NGCP::BulkProcessor::FileProcessors::XslxFileSimple;
use strict;

## no critic

use Archive::Zip qw(AZ_OK);
use Spreadsheet::ParseXLSX qw();
use XML::LibXML qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
    fileprocessingstarted
    fileprocessingdone
    lines_read
    processing_lines
);

use NGCP::BulkProcessor::LogError qw(
    fileprocessingfailed
    fileerror
);

use NGCP::BulkProcessor::Utils qw(threadid);

use NGCP::BulkProcessor::FileProcessor qw(create_process_context);

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $default_sheet_name = undef; #'~';
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    #$self->{numofthreads} = shift // $default_numofthreads;
    $self->{custom_formats} = shift;
    $self->{sheet_name} = shift // $default_sheet_name;
    #$self->{header_row} = shift // 0;
    $self->{blocksize} = $blocksize;

    bless($self,$class);

    return $self;

}

sub process {

    my $self = shift;

    my %params = @_;
    my ($file,
        $process_code,
        $static_context,
        $init_process_context_code,
        $uninit_process_context_code) = @params{qw/
            file
            process_code
            static_context
            init_process_context_code
            uninit_process_context_code
        /};

    fileprocessingstarted($file,getlogger(__PACKAGE__));
    my $result = 0;
    my $tid = threadid();
    my $context = create_process_context($static_context,{ instance => $self,
        filename => $file,
        tid      => $tid,
    });
    eval {
        my $parser = Spreadsheet::ParseXLSX->new();
        my ($workbook,$error) = _read_workbook($parser,$file);
        if (defined $init_process_context_code and 'CODE' eq ref $init_process_context_code) {
            &$init_process_context_code($context);
        }
        if (not defined $workbook) {
            fileerror('processing file - error reading file ' . $file . ': ' . $error,getlogger(__PACKAGE__));
        } else {
            my $sheet;
            if ($self->{sheet_name}) {
                # like Excel::Reader::XLSX, the last sheet of a duplicate name wins:
                foreach my $candidate (@{$workbook->{sheets}}) {
                    $sheet = $candidate if $candidate->{name} eq $self->{sheet_name};
                }
            } else {
                $sheet = $workbook->{sheets}->[0];
            }
            if (not defined $sheet) {
                fileerror('invalid spreadsheet',getlogger(__PACKAGE__));
            } else {
                my $rows = _read_sheet_rows($workbook,$sheet);
                $result = 1;

                my $i = 0;
                processing_lines($tid,$i,$self->{blocksize},undef,getlogger(__PACKAGE__));
                my @rows = ();
                foreach my $row (@$rows) {
                    last unless $result;
                    push(@rows,$row);
                    if ((scalar @rows) >= $self->{blocksize}) {
                        $result &= &$process_code($context,\@rows,$i);
                        $i += scalar @rows;
                        processing_lines($tid,$i,$self->{blocksize},undef,getlogger(__PACKAGE__));
                        @rows = ();
                    }
                }
                $result &= &$process_code($context,\@rows,$i);
            }
        }
    };
    $result &= 0 if $@;
    eval {
        if (defined $uninit_process_context_code and 'CODE' eq ref $uninit_process_context_code) {
            &$uninit_process_context_code($context);
        }
    };
    if ($result) {
        fileprocessingdone($file,getlogger(__PACKAGE__));
    } else {
        fileprocessingfailed($file,getlogger(__PACKAGE__));
    }
    return $result;

}

# The rows are read from the package xml directly, to yield the raw cell
# scalars of Excel::Reader::XLSX next_row()->values():
# - one array per <row> element, in document order (no gap rows),
# - the array ends at the last <c> column, missing (never written) columns
#   are undef, a <c> without <v> is '',
# - the raw <v> text (no number formats, no numeric coercion),
# - shared strings and inline strings as the element's text content,
# - a <c> without r attribute ends the row.
# Spreadsheet::ParseXLSX is only used to open and validate the workbook.

my $REL_NS = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships';

sub _read_workbook {

    my ($parser,$file) = @_;

    my $workbook;
    eval {
        $parser->parse($file);
        my $zip = Archive::Zip->new();
        die("cannot read zip\n") unless $zip->read($file) == AZ_OK;
        my $parts = _content_types($zip);
        my $workbook_path = $parts->{workbook} // 'xl/workbook.xml';
        my $workbook_root = $workbook_path;
        $workbook_root =~ s{[^/]*$}{};
        my $rels_path = $workbook_path;
        $rels_path =~ s{workbook\.xml$}{_rels/workbook.xml.rels};
        my $rels = _relationships($zip,$rels_path);
        my $doc = _xml($zip,$workbook_path);
        die("$workbook_path missing\n") unless defined $doc;
        my @sheets = ();
        foreach my $node ($doc->findnodes('//*[local-name()="sheets"]/*[local-name()="sheet"]')) {
            my $rid = $node->getAttributeNS($REL_NS,'id');
            $rid = $node->getAttribute('r:id') unless defined $rid;
            my $target = (defined $rid ? $rels->{$rid} : undef);
            push(@sheets,{
                name => $node->getAttribute('name') // '',
                path => (defined $target ? _part_path($workbook_root,$target) : undef),
            });
        }
        $workbook = {
            zip => $zip,
            shared_strings_path => $parts->{shared_strings} // 'xl/sharedStrings.xml',
            sheets => \@sheets,
        };
    };
    if ($@) {
        my $error = $@;
        $error =~ s/\s+\z//;
        return (undef,$error);
    }
    return ($workbook,undef);

}

sub _read_sheet_rows {

    my ($workbook,$sheet) = @_;

    my @rows = ();
    return \@rows unless defined $sheet->{path};
    my $doc = _xml($workbook->{zip},$sheet->{path});
    return \@rows unless defined $doc;

    my $strings = _shared_strings($workbook->{zip},$workbook->{shared_strings_path});

    foreach my $row ($doc->findnodes('//*[local-name()="row"]')) {
        my @values = ();
        my $max = -1;
        foreach my $cell ($row->findnodes('./*[local-name()="c"]')) {
            my $ref = $cell->getAttribute('r');
            last unless $ref;
            my $col = _col_index($ref);
            last unless defined $col;
            my $value = '';
            foreach my $child ($cell->childNodes()) {
                next unless $child->nodeType == XML::LibXML::XML_ELEMENT_NODE;
                my $name = $child->localname();
                $value = $child->textContent() if ($name eq 'v' or $name eq 'is');
            }
            if (($cell->getAttribute('t') // '') eq 's') {
                $value = (defined $strings->[$value] ? $strings->[$value] : '');
            }
            $values[$col] = $value;
            $max = $col if $col > $max;
        }
        my @row = ();
        @row = @values[0 .. $max] if $max >= 0;
        push(@rows,\@row);
    }

    return \@rows;

}

sub _shared_strings {

    my ($zip,$path) = @_;

    my @strings = ();
    my $doc = _xml($zip,$path);
    return \@strings unless defined $doc;
    foreach my $si ($doc->findnodes('//*[local-name()="si"]')) {
        push(@strings,$si->textContent());
    }
    return \@strings;

}

sub _content_types {

    my ($zip) = @_;

    my %parts = ();
    my $doc = _xml($zip,'[Content_Types].xml');
    return \%parts unless defined $doc;
    foreach my $node ($doc->findnodes('//*[local-name()="Override"]')) {
        my $part = $node->getAttribute('PartName') // '';
        $part =~ s{^/}{};
        if ($part =~ /sharedStrings\.xml$/) {
            $parts{shared_strings} = $part;
        } elsif ($part =~ /workbook\.xml$/) {
            $parts{workbook} = $part;
        }
    }
    return \%parts;

}

sub _relationships {

    my ($zip,$path) = @_;

    my %rels = ();
    my $doc = _xml($zip,$path);
    return \%rels unless defined $doc;
    foreach my $node ($doc->findnodes('//*[local-name()="Relationship"]')) {
        my $id = $node->getAttribute('Id');
        $rels{$id} = $node->getAttribute('Target') if defined $id;
    }
    return \%rels;

}

sub _part_path {

    my ($root,$target) = @_;

    $target =~ s{\\}{/}g;
    my $path = ($target =~ s{^/}{} ? $target : $root . $target);
    my @segments = ();
    foreach my $segment (split(m{/},$path)) {
        next if $segment eq '' or $segment eq '.';
        if ($segment eq '..') {
            pop(@segments);
        } else {
            push(@segments,$segment);
        }
    }
    return join('/',@segments);

}

sub _col_index {

    my ($ref) = @_;

    return undef unless $ref =~ /^([A-Za-z]+)/;
    my $col = 0;
    foreach my $char (split(//,uc($1))) {
        $col = $col * 26 + (ord($char) - ord('A') + 1);
    }
    return $col - 1;

}

sub _xml {

    my ($zip,$path) = @_;

    my $member = $zip->memberNamed($path);
    return undef unless defined $member;
    my ($xml,$status) = $member->contents();
    die("cannot read $path\n") unless $status == AZ_OK;
    my $parser = XML::LibXML->new(
        no_network => 1,
        expand_entities => 0,
        load_ext_dtd => 0,
    );
    return $parser->parse_string($xml);

}

1;
