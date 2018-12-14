package NGCP::BulkProcessor::FileProcessors::XslxFileSimple;
use strict;

## no critic

use Spreadsheet::Reader::ExcelXML qw();

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

my $default_sheet_name = '~';
my $blocksize = 100;

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::FileProcessor->new(@_);

    #$self->{numofthreads} = shift // $default_numofthreads;
    $self->{custom_formats} = shift;
    $self->{sheet_name} = shift // $default_sheet_name;
    $self->{header_row} = shift // 0;
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
    eval {
        my $workbook = Spreadsheet::Reader::ExcelXML->new(
            file => $file,
            #group_return_type => 'value',
            count_from_zero => 0,
            values_only => 1,
            empty_is_end => 1,
            group_return_type => ('HASH' eq ref $self->{custom_formats} ? 'value' : 'xml_value'),
            from_the_edge => 0,
            empty_return_type => 'undef_string',
            spaces_are_empty => 1,
            merge_data => 0,
            column_formats => 0,
        );
        my $tid = threadid();
        my $context = _create_process_context($static_context,{ instance => $self,
            filename => $file,
            tid      => $tid,
        });
        if (defined $init_process_context_code and 'CODE' eq ref $init_process_context_code) {
            &$init_process_context_code($context);
        }
        if (not $workbook->file_opened) {
            fileerror('processing file - error reading file ' . $file . ': ' . $workbook->error(),getlogger(__PACKAGE__));
        } else {
            my $worksheet = $workbook->worksheet($self->{sheet_name});
            if (not defined $worksheet) {
                fileerror('processing file - error reading file ' . $file . ': ' . $workbook->error(),getlogger(__PACKAGE__));
            } else {
                $result = 1;
                #_info($context,"worksheet '" . $worksheet->get_name() . "' opened");
                processing_lines($tid,$i,$self->{blocksize},undef,getlogger(__PACKAGE__));
                $worksheet->set_custom_formats($self->{custom_formats}) if 'HASH' eq ref $self->{custom_formats};
                #$worksheet->set_custom_formats({
                #    2 =>'yyyy-mm-dd',
                #});
                $worksheet->set_headers($self->{header_row}) if defined $self->{header_row};
                if ($worksheet->header_row_set()) {
                    $worksheet->go_to_or_past_row($worksheet->get_excel_position($worksheet->get_last_header_row()));
                }
                my $i = 0;
                my $value;
                my @rows = ();
                while ($result) {
                    $value = $worksheet->fetchrow_arrayref;
                    last if (not $value or 'EOF' eq $value);
                    push(@rows,$value);
                    $i++;
                    if ((scalar @rows) >= $self->{blocksize}) {
                        $result &= &$process_code($context,\@rows,$i);
                        @rows = ();
                        processing_lines($tid,$i,$self->{blocksize},undef,getlogger(__PACKAGE__));
                    }
                }
                $result &= &$process_code($context,\@rows);
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

1;