package NGCP::BulkProcessor::Projects::Migration::Teletek::FileProcessors::CSVFile;
use strict;

## no critic
use Encode qw();

use NGCP::BulkProcessor::Logging qw(
    getlogger
);
use NGCP::BulkProcessor::LogError qw(
    fileprocessingerror
    fileprocessingwarn
);

use NGCP::BulkProcessor::FileProcessor;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::FileProcessor);
our @EXPORT_OK = qw();

my $default_lineseparator = "\n";
my $default_fieldseparator = ",";
my $default_encoding = 'UTF-8';

my $buffersize = 100 * 1024;
my $threadqueuelength = 10;
my $default_numofthreads = 3;
#my $multithreading = 0;
my $blocksize = 100;

my $default_quotechar = '"';
my $default_escapequotesequence = '""';
my $default_commentchar = '#';

my $LINE = 0;
my $FIELD_START = 1;
my $FIELD_STOP = 2;

my $error_sample_length = 30;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    $self->{numofthreads} = shift // $default_numofthreads;
    $self->{line_separator} = shift // $default_lineseparator;
    $self->{field_separator} = shift // $default_fieldseparator;
    $self->{encoding} = shift // $default_encoding;
    $self->{buffersize} = $buffersize;
    $self->{threadqueuelength} = $threadqueuelength;
    #$self->{multithreading} = $multithreading;
    $self->{blocksize} = $blocksize;

    $self->{quote_char} = shift // $default_quotechar;
    $self->{escape_quote_sequence} = shift // $default_escapequotesequence;
    $self->{comment_char} = shift // $default_commentchar;
    $self->{line_number} = 1;

    bless($self,$class);

    #restdebug($self,__PACKAGE__ . ' file processor created',getlogger(__PACKAGE__));

    return $self;

}

sub extractlines {
    my ($context,$buffer_ref,$lines) = @_;

    my $length = length($$buffer_ref);
    my $escape_quote_sequence_length = length($context->{instance}->{escape_quote_sequence});
    my $quote_char_length = length($context->{instance}->{quote_char});
    my $escape_by_doubling = ($context->{instance}->{escape_quote_sequence} eq ($context->{instance}->{quote_char} x 2));
    my $field_separator_length = length($context->{instance}->{field_separator});
    my $line_separator_length = length($context->{instance}->{line_separator});
    my $comment_char_length = length($context->{instance}->{comment_char});
    my $pos = 0;
    my $last_line_pos = 0;
    my $field = undef;
    my @fields = ();
    my $state = $LINE;
    while ($pos < $length) {
        if ($context->{instance}->{escape_quote_sequence} eq substr($$buffer_ref,$pos,$escape_quote_sequence_length)) {
            if ($state == $FIELD_START) {
                $field .= $context->{instance}->{quote_char};
            } elsif ($state == $LINE and $escape_by_doubling) {
                $field = '';
                $state = $FIELD_STOP;
            } else {
                fileprocessingerror($context->{filename},"csv parse error at line $context->{instance}->{line_number} near " . substr($$buffer_ref,$pos,$error_sample_length)
                    . '...',getlogger(__PACKAGE__)); # : escaped field separator symbol not allowed here
                return 0;
            }
            $pos += $escape_quote_sequence_length;
        } elsif ($context->{instance}->{quote_char} eq substr($$buffer_ref,$pos,$quote_char_length)) {
            if ($state == $LINE) {
                $field = '';
                $state = $FIELD_START;
            } elsif ($state == $FIELD_START) {
                $state = $FIELD_STOP;
            } else {
                fileprocessingerror($context->{filename},"csv parse error at line $context->{instance}->{line_number} near " . substr($$buffer_ref,$pos,$error_sample_length)
                    . '...',getlogger(__PACKAGE__)); # : unescaped field separator symbol
                return 0;
            }
            $pos += $quote_char_length;
        } elsif ($context->{instance}->{field_separator} eq substr($$buffer_ref,$pos,$field_separator_length)) {
            if ($state == $FIELD_START) {
                $field .= $context->{instance}->{field_separator};
            } elsif ($state == $FIELD_STOP or $state == $LINE) {
                push(@fields,$context->{instance}->_encode($field));
                $field = undef;
                $state = $LINE;
            }
            $pos += $field_separator_length;
        } elsif ($context->{instance}->{line_separator} eq substr($$buffer_ref,$pos,$line_separator_length)) {
            if ($state == $FIELD_START) {
                $field .= $context->{instance}->{line_separator};
            } elsif ($state == $FIELD_STOP) {
                push(@fields,$context->{instance}->_encode($field));
                $field = undef;
                push(@$lines,[@fields]);
                @fields = ();
                $context->{instance}->{line_number} += 1;
                $last_line_pos = $pos + $line_separator_length;
                $state = $LINE;
            } elsif ($state == $LINE) {
                push(@fields,$context->{instance}->_encode($field)) if (scalar @fields) > 0;
                push(@$lines,[@fields]);
                @fields = ();
                $context->{instance}->{line_number} += 1;
                $last_line_pos = $pos + $line_separator_length;
            }
            $pos += $line_separator_length;
        } elsif ($comment_char_length > 0 and ($context->{instance}->{comment_char} eq substr($$buffer_ref,$pos,$comment_char_length))) {
            if ($state == $FIELD_START) {
                $field .= $context->{instance}->{comment_char};
                $pos += $comment_char_length;
            } elsif ($state == $FIELD_STOP) {
                push(@fields,$context->{instance}->_encode($field));
                $field = undef;
                push(@$lines,[@fields]);
                @fields = ();
                $context->{instance}->{line_number} += 1;
                $pos = index($$buffer_ref,$context->{instance}->{line_separator},$pos) + $line_separator_length;
                $last_line_pos = $pos;
                $state = $LINE;
            } elsif ($state == $LINE) {
                push(@fields,$context->{instance}->_encode($field)) if (scalar @fields) > 0;
                push(@$lines,[@fields]);
                @fields = ();
                $context->{instance}->{line_number} += 1;
                $pos = index($$buffer_ref,$context->{instance}->{line_separator},$pos) + $line_separator_length;
                $last_line_pos = $pos;
            }
        } else {
            if ($state == $FIELD_START) {
                $field .= substr($$buffer_ref,$pos,1);
            } else {
                fileprocessingerror($context->{filename},"csv parse error at line $context->{instance}->{line_number} near " . substr($$buffer_ref,$pos,$error_sample_length)
                    . '...',getlogger(__PACKAGE__)); # : value not enclosed by field separator symbol
                return 0;
            }
            $pos += 1;
        }
    }

    if ($last_line_pos < $length) {
        $$buffer_ref = substr($$buffer_ref,$last_line_pos);
    } else {
        $$buffer_ref = undef;
    }

    return 1;

}

sub extractfields {
    my ($context,$line_ref) = @_;
    if ('ARRAY' eq ref $line_ref) {
        return $line_ref; #[ map { $context->{instance}->_encode($_); } @$line_ref ];
    } else {
        my @last_line = ();
        extractlines($context,$line_ref,\@last_line);
        return $last_line[0];
        #return [ map { $context->{instance}->_encode($_); } @{$last_line[0]} ];
    }
}

sub _encode {
    my $self = shift;
    return Encode::encode($self->{encoding}, shift); #"UTF-8"
}

1;
