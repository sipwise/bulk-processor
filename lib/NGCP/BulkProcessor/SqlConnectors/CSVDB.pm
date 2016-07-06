package NGCP::BulkProcessor::SqlConnectors::CSVDB;
use strict;

## no critic

#use File::Basename;
#use Cwd;
#use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use NGCP::BulkProcessor::Globals qw(
    $LongReadLen_limit
    $csv_path);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    dbdebug
    dbinfo
    xls2csvinfo
    texttablecreated
    indexcreated
    tabletruncated
    tabledropped);

use NGCP::BulkProcessor::LogError qw(
    dberror
    dbwarn
    fieldnamesdiffer
    fileerror
    filewarn
    xls2csverror
    xls2csvwarn);

use NGCP::BulkProcessor::Array qw(contains setcontains);

use NGCP::BulkProcessor::Utils qw(makepath changemod chopstring);

use NGCP::BulkProcessor::SqlConnector;

use DBI;
use DBD::CSV 0.26;
use File::Path qw(remove_tree);
use Locale::Recode;
use Spreadsheet::ParseExcel;
use Spreadsheet::ParseExcel::FmtUnicode;
use Excel::Reader::XLSX;
use Text::CSV_XS;
use File::Basename;
use MIME::Parser;
use HTML::PullParser qw();
use HTML::Entities qw(decode_entities);
use IO::Uncompress::Unzip qw(unzip $UnzipError);

# no debian package yet:
#use DateTime::Format::Excel;

require Exporter;
our @ISA = qw(Exporter NGCP::BulkProcessor::SqlConnector);
our @EXPORT_OK = qw(
    cleanupcvsdirs
    xlsbin2csv
    xlsxbin2csv
    sanitize_column_name
    sanitize_spreadsheet_name
    get_tableidentifier
    $csvextension);
#excel_to_timestamp
#excel_to_date

our $csvextension = '.csv';

my $default_csv_config = { eol         => "\r\n",
                            sep_char    => ';',
                            quote_char  => '"',
                            escape_char => '"',
                          };

my @TABLE_TAGS = qw(table tr td);

my $LongReadLen = $LongReadLen_limit; #bytes
my $LongTruncOk = 0;

#my $logger = getlogger(__PACKAGE__);

#my $lock_do_chunk = 0;
#my $lock_get_chunk = 0;
my $rowblock_transactional = 0;

my $invalid_excel_spreadsheet_chars_pattern = '[' . quotemeta('[]:*?/\\') . ']';

sub sanitize_spreadsheet_name { #Invalid character []:*?/\ in worksheet name
    my $spreadsheet_name = shift;
    $spreadsheet_name =~ s/$invalid_excel_spreadsheet_chars_pattern//g;
    return chopstring($spreadsheet_name,31); #Sheetname eventually inconsistent etc. must be <= 31 chars
}

sub sanitize_column_name {
    my $column_name = shift;
    $column_name =~ s/\W/_/g;
    return $column_name;
}

#sub excel_to_date {
#    my $excel_date_value = shift;
#    if ($excel_date_value > 0) {
#        my $datetime = DateTime::Format::Excel->parse_datetime($excel_date_value);
#        return $datetime->ymd('-'); # prints 1992-02-28
#    }
#    return undef;
#}

#sub excel_to_timestamp {
#    my $excel_datetime_value = shift;
#    if ($excel_datetime_value > 0) {
#        my $datetime = DateTime::Format::Excel->parse_datetime($excel_datetime_value);
#        return $datetime->ymd('-') . ' ' . $datetime->hms(':');
#    }
#    return undef;
#}

sub new {

    my $class = shift;

    my $self = NGCP::BulkProcessor::SqlConnector->new(@_);

    $self->{db_dir} = undef;
    $self->{f_dir} = undef;
    $self->{csv_tables} = undef;
    $self->{files} = undef;

    $self->{drh} = DBI->install_driver('CSV');

    bless($self,$class);

    dbdebug($self,__PACKAGE__ . ' connector created',getlogger(__PACKAGE__));

    return $self;

}

sub _connectidentifier {

    my $self = shift;
    return $self->{f_dir};

}

sub tableidentifier {

    my $self = shift;
    my $tablename = shift;
    return $tablename;

}

sub columnidentifier {

    my $self = shift;
    my $columnname = shift;

    return sanitize_column_name($columnname); #actually happens automatically by dbd::csv

}

sub get_tableidentifier {

    my ($tablename,$db_dir) = @_;
    if (defined $db_dir) {
        return $db_dir . '.' . $tablename;
    } else {
        return $tablename;
    }

}

sub getsafetablename {

    my $self = shift;
    my $tableidentifier = shift;
    return lc($self->SUPER::getsafetablename($tableidentifier));

}

sub getdatabases {

    my $self = shift;

    local *DBDIR;
    if (not opendir(DBDIR, $csv_path)) {
        fileerror('cannot opendir ' . $csv_path . ': ' . $!,getlogger(__PACKAGE__));
        return [];
    }
    my @dirs = grep { $_ ne '.' && $_ ne '..' && -d $csv_path . $_ } readdir(DBDIR);
    closedir DBDIR;
    my @databases = ();
    foreach my $dir (@dirs) {
        push @databases,$dir;
    }
    return \@databases;

}

sub _createdatabase {

    my $self = shift;
    my ($db_dir) = @_;

    my $f_dir; # = _get_f_dir($db_dir);
    if (length($db_dir) > 0) {
        $f_dir = $csv_path . $db_dir . '/';
    } else {
        $f_dir = $csv_path;
    }

    dbinfo($self,'opening csv folder',getlogger(__PACKAGE__));

    #mkdir $f_dir;
    makepath($f_dir,\&fileerror,getlogger(__PACKAGE__));

    #if (not -d $f_dir) {
    #    fileerror('cannot opendir ' . $f_dir . ': ' . $!,getlogger(__PACKAGE__));
    #    return;
    #}

    #local *DBDIR;
    #if (not opendir(DBDIR, $f_dir)) {
    #    fileerror('cannot opendir ' . $f_dir . ': ' . $!,getlogger(__PACKAGE__));
    #    return;
    #}
    #closedir DBDIR;

    #changemod($f_dir);

    return $f_dir;
}

sub db_connect {

    my $self = shift;

    my ($db_dir,$csv_tables) = @_;

    $self->SUPER::db_connect($db_dir,$csv_tables);

    $self->{db_dir} = $db_dir;
    $self->{csv_tables} = $csv_tables;
    $self->{f_dir} = $self->_createdatabase($db_dir);

    my $dbh_config = {
            f_schema        => undef,
            #f_lock           => 0, n/a in 0.26 yet?
            cvs_eol         => $default_csv_config->{eol},
            cvs_sep_char    => $default_csv_config->{sep_char},
            cvs_quote_char  => $default_csv_config->{quote_char},
            cvs_escape_char => $default_csv_config->{escape_char},
            PrintError      => 0,
            RaiseError      => 0,
        };
    my $usetabledef = 0;
    if (defined $csv_tables and ref $csv_tables eq 'HASH') {
        $usetabledef = 1;
    } else {
        $dbh_config->{f_dir} = $self->{f_dir};
        $dbh_config->{f_ext} = $csvextension . '/r';
    }

    my $dbh = DBI->connect ('dbi:CSV:','','',$dbh_config) or
        dberror($self,'error connecting: ' . $self->{drh}->errstr(),getlogger(__PACKAGE__));

    $dbh->{InactiveDestroy} = 1;

    $dbh->{LongReadLen} = $LongReadLen;
    $dbh->{LongTruncOk} = $LongTruncOk;

    $self->{dbh} = $dbh;

    if ($usetabledef) {
        my @files = ();
        foreach my $tablename (keys %$csv_tables) {
            $dbh->{csv_tables}->{$tablename} = $csv_tables->{$tablename};
            push @files,$csv_tables->{$tablename}->{file};
            dbinfo($self,'using ' . $csv_tables->{$tablename}->{file},getlogger(__PACKAGE__));
        }
        $self->{files} = \@files;
    } else {
        my @tablenames = $self->_list_tables();
        foreach my $tablename (@tablenames) {
            $dbh->{csv_tables}->{$tablename} = { eol         => $default_csv_config->{eol},
                                                 sep_char    => $default_csv_config->{sep_char},
                                                 quote_char  => $default_csv_config->{quote_char},
                                                 escape_char => $default_csv_config->{escape_char},
                                               }
        }
    }

    dbinfo($self,'connected',getlogger(__PACKAGE__));

}


sub _list_tables {
    my $self = shift;
    my @table_list;

    eval {
        @table_list = map { local $_ = $_; s/^\.\///g; $_; } $self->{dbh}->func('list_tables');
    };
    if ($@) {
        my @tables;
        eval {
            @tables = $self->{dbh}->func("get_avail_tables") or return;
        };
        if ($@) {
              dberror($self,'error listing csv tables: ' . $@,getlogger(__PACKAGE__));
        } else {
            foreach my $ref (@tables) {
                if (defined $ref) {
                    if (ref $ref eq 'ARRAY') {
                        push @table_list, $ref->[2];
                    #} else {
                    #    push @table_list, $ref;
                    }
                }
            }
        }
    }

    return @table_list; #removeduplicates(\@table_list);
}

sub _db_disconnect {

    my $self = shift;

    $self->SUPER::_db_disconnect();

}

sub vacuum {

    my $self = shift;
    my $tablename = shift;

}

sub cleanupcvsdirs {

    my (@remainingdbdirs) = @_;
    local *DBDIR;
    if (not opendir(DBDIR, $csv_path)) {
        fileerror('cannot opendir ' . $csv_path . ': ' . $!,getlogger(__PACKAGE__));
        return;
    }
    my @dirs = grep { $_ ne '.' && $_ ne '..' && -d $csv_path . $_ } readdir(DBDIR);
    closedir DBDIR;
    my @remainingdbdirectories = ();
    foreach my $dirname (@remainingdbdirs) {
        push @remainingdbdirectories,$csv_path . $dirname . '/';
    }
    foreach my $dir (@dirs) {
        #print $file;
        my $dirpath = $csv_path . $dir . '/';
        if (not contains($dirpath,\@remainingdbdirectories)) {
            #if (remove_tree($dirpath) == 0) {
            #    filewarn('cannot remove ' . $dirpath . ': ' . $!,getlogger(__PACKAGE__));
            #}
            remove_tree($dirpath, {
                'keep_root' => 0,
                'verbose' => 1,
                'error' => \my $err });
            if (@$err) {
                for my $diag (@$err) {
                    my ($file, $message) = %$diag;
                    if ($file eq '') {
                        filewarn("cleanup: $message",getlogger(__PACKAGE__));
                    } else {
                        filewarn("problem unlinking $file: $message",getlogger(__PACKAGE__));
                    }
                }
            }
        }
    }

}

sub getfieldnames {

    my $self = shift;
    my $tablename = shift;

    my $fieldnames = [];

    if (defined $self->{dbh}) {

        my $query = 'SELECT * FROM ' . $self->tableidentifier($tablename) . ' LIMIT 1';
        dbdebug($self,'getfieldnames: ' . $query,getlogger(__PACKAGE__));
        my $sth = $self->{dbh}->prepare($query) or $self->_prepare_error($query);
        $sth->execute() or $self->_execute_error($query,$sth,());
        $fieldnames = $sth->{NAME};
        $sth->finish();

    }

    return $fieldnames;

}

sub getprimarykeycols {

    my $self = shift;
    my $tablename = shift;
    return [];

}

sub create_primarykey {

    my $self = shift;
    my ($tablename,$keycols,$fieldnames) = @_;

    return 0;
}
sub create_indexes {

    my $self = shift;
    my ($tablename,$indexes,$keycols) = @_;

    return 0;
}

sub _gettablefilename {

    my $self = shift;
    my $tablename = shift;
    return $self->{f_dir} . $tablename . $csvextension;

}

sub create_texttable {

    my $self = shift;
    my ($tablename,$fieldnames,$keycols,$indexes,$truncate) = @_;

    if (length($tablename) > 0 and defined $fieldnames and ref $fieldnames eq 'ARRAY') {

        my $created = 0;
        if ($self->table_exists($tablename) == 0) {

            if (not exists $self->{dbh}->{csv_tables}->{$tablename}) {
                $self->{dbh}->{csv_tables}->{$tablename} = { eol         => $default_csv_config->{eol},
                                                             sep_char    => $default_csv_config->{sep_char},
                                                             quote_char  => $default_csv_config->{quote_char},
                                                             escape_char => $default_csv_config->{escape_char},
                                                           };
            }

            my $statement = 'CREATE TABLE ' . $self->tableidentifier($tablename) . ' (';
            $statement .= join(' TEXT, ',map { local $_ = $_; $_ = $self->columnidentifier($_); $_; } @$fieldnames) . ' TEXT';
            $statement .= ')';

            $self->db_do($statement);

            changemod($self->_gettablefilename($tablename));

            texttablecreated($self,$tablename,getlogger(__PACKAGE__));

            $created = 1;
        } else {
            my $fieldnamesfound = $self->getfieldnames($tablename);
            if (not setcontains($fieldnames,$fieldnamesfound,1)) {
                fieldnamesdiffer($self,$tablename,$fieldnames,$fieldnamesfound,getlogger(__PACKAGE__));
                return 0;
            }
        }

        if (not $created and $truncate) {
            $self->truncate_table($tablename);
        }
        return 1;
    } else {
        return 0;
    }

}

sub multithreading_supported {

    my $self = shift;
    return 0;

}

sub truncate_table {

    my $self = shift;
    my $tablename = shift;

    $self->db_do('DELETE FROM ' . $self->tableidentifier($tablename));
    tabletruncated($self,$tablename,getlogger(__PACKAGE__));

}

sub table_exists {

    my $self = shift;
    my $tablename = shift;

    if (defined $self->{dbh}) {
        my @tables = $self->_list_tables();
        return contains($tablename,\@tables);
    }

    return undef;

}

sub drop_table {

    my $self = shift;
    my $tablename = shift;

    if ($self->table_exists($tablename) > 0) {
        $self->db_do('DROP TABLE ' . $self->tableidentifier($tablename));
        delete $self->{dbh}->{csv_tables}->{$tablename};
        tabledropped($self,$tablename,getlogger(__PACKAGE__));
        return 1;
    }
    return 0;

}

sub db_begin {

    my $self = shift;
    if (defined $self->{dbh}) {
        dbdebug($self, "transactions not supported",getlogger(__PACKAGE__));
    }

}

sub db_commit {

    my $self = shift;
    if (defined $self->{dbh}) {
        dbdebug($self, "transactions not supported",getlogger(__PACKAGE__));
    }

}

sub db_rollback {

    my $self = shift;
    if (defined $self->{dbh}) {
        dbdebug($self, "transactions not supported",getlogger(__PACKAGE__));
    }

}

sub db_do_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;

    $self->SUPER::db_do_begin($query,$rowblock_transactional,@_);

}

sub db_get_begin {

    my $self = shift;
    my $query = shift;
    #my $tablename = shift;

    $self->SUPER::db_get_begin($query,$rowblock_transactional,@_);

}

sub db_finish {

    my $self = shift;

    $self->SUPER::db_finish($rowblock_transactional);

}

sub xlsbin2csv {

    my ($inputfile,$outputfile,$worksheetname,$sourcecharset) = @_;

    return _convert_xlsbin2csv($inputfile,
                            $worksheetname,
                            $sourcecharset,
                            $outputfile,
                            'UTF-8',
                            $default_csv_config->{quote_char},
                            $default_csv_config->{escape_char},
                            $default_csv_config->{sep_char},
                            $default_csv_config->{eol});

}

sub _convert_xlsbin2csv {

    my ($SourceFilename,$worksheet,$SourceCharset,$DestFilename,$DestCharset,$quote_char,$escape_char,$sep_char,$eol) = @_;

    my $csvlinecount = 0;

    xls2csvinfo('start converting ' . $SourceFilename . ' (worksheet ' . $worksheet . ') to ' . $DestFilename . ' ...',getlogger(__PACKAGE__));

    $SourceCharset = 'UTF-8' unless $SourceCharset;
    $DestCharset = $SourceCharset unless $DestCharset;

    xls2csvinfo('reading ' . $SourceFilename . ' as ' . $SourceCharset,getlogger(__PACKAGE__));

    my $XLS = new IO::File;
    if (not $XLS->open('<' . $SourceFilename)) {
        fileerror('cannot open file ' . $SourceFilename . ': ' . $!,getlogger(__PACKAGE__));
        return 0;
    }

    my $Formatter = Spreadsheet::ParseExcel::FmtUnicode->new(Unicode_Map => $SourceCharset);

    my $parser   = Spreadsheet::ParseExcel->new();
    my $Book = $parser->parse($XLS,$Formatter); #$SourceFilename

    if ( !defined $Book ) {
        xls2csverror($parser->error(),getlogger(__PACKAGE__));
        #die $parser->error(), ".\n";
        $XLS->close();
        return 0;
    }

    #my $Book = Spreadsheet::ParseExcel::Workbook->Parse($XLS, $Formatter) or xls2csverror('can\'t read spreadsheet',getlogger(__PACKAGE__));

    my $Sheet;
    if ($worksheet) {

        #my $test = $Book->GetContent();

    $Sheet = $Book->Worksheet($worksheet);
    if (!defined $Sheet) {
            xls2csverror('invalid spreadsheet',getlogger(__PACKAGE__));
            return 0;
        }
    #unless ($O{'q'})
    #{
    #   print qq|Converting the "$Sheet->{Name}" worksheet.\n|;
    #}
        xls2csvinfo('converting the ' . $Sheet->{Name} . ' worksheet',getlogger(__PACKAGE__));
    } else {
    ($Sheet) = @{$Book->{Worksheet}};
    if ($Book->{SheetCount}>1) {
        #print qq|Multiple worksheets found. Will convert the "$Sheet->{Name}" worksheet.\n|;
            xls2csvinfo('multiple worksheets found, converting ' . $Sheet->{Name},getlogger(__PACKAGE__));
    }
    }

    unlink $DestFilename;
    local *CSV;
    if (not open(CSV,'>' . $DestFilename)) {
        fileerror('cannot open file ' . $DestFilename . ': ' . $!,getlogger(__PACKAGE__));
        $XLS->close();
        return 0;
    }
    binmode CSV;

    my $Csv = Text::CSV_XS->new({
            'quote_char'  => $quote_char,
            'escape_char' => $escape_char,
            'sep_char'    => $sep_char,
            'binary'      => 1,
    });

    my $Recoder;
    if ($DestCharset) {
    $Recoder = Locale::Recode->new(from => $SourceCharset, to => $DestCharset);
    }

    for (my $Row = $Sheet->{MinRow}; defined $Sheet->{MaxRow} && $Row <= $Sheet->{MaxRow}; $Row++) {
    my @Row;
    for (my $Col = $Sheet->{MinCol}; defined $Sheet->{MaxCol} && $Col <= $Sheet->{MaxCol}; $Col++) {
        my $Cell = $Sheet->{Cells}[$Row][$Col];

        my $Value = "";
            if ($Cell) {
        $Value = $Cell->Value;
        if ($Value eq 'GENERAL') {
            # Sometimes numbers are read incorrectly as "GENERAL".
                    # In this case, the correct value should be in ->{Val}.
                    $Value = $Cell->{Val};
        }
        if ($DestCharset) {
            $Recoder->recode($Value);
        }
        }

            # We assume the line is blank if there is nothing in the first column.
            last if $Col == $Sheet->{MinCol} and !$Value;

            push(@Row,$Value);
    }

    next unless @Row;

    my $Status = $Csv->combine(@Row);

    if (!defined $Status) {
            xls2csvwarn('csv error: ' . $Csv->error_input(),getlogger(__PACKAGE__));
    }

    if (defined $Status) {
            print CSV $Csv->string();
            if ($Row < $Sheet->{MaxRow}) {
        print CSV $eol;
            }
            $csvlinecount++;
    }
    }

    close CSV;
    $XLS->close;

    xls2csvinfo($csvlinecount . ' line(s) converted',getlogger(__PACKAGE__));

    return $csvlinecount;

}

sub xlsxbin2csv {

    my ($inputfile,$outputfile,$worksheetname) = @_;

    return _convert_xlsxbin2csv($inputfile,
                            $worksheetname,
                            $outputfile,
                            'UTF-8',
                            $default_csv_config->{quote_char},
                            $default_csv_config->{escape_char},
                            $default_csv_config->{sep_char},
                            $default_csv_config->{eol});

}

sub _convert_xlsxbin2csv {
    my ($SourceFilename,$worksheet,$DestFilename,$DestCharset,$quote_char,$escape_char,$sep_char,$eol) = @_;

    my $csvlinecount = 0;

    xls2csvinfo('start converting ' . $SourceFilename . ' (worksheet ' . $worksheet . ') to ' . $DestFilename . ' ...',getlogger(__PACKAGE__));


    my $XLS = new IO::File;
    if (not $XLS->open('<' . $SourceFilename)) {
        fileerror('cannot open file ' . $SourceFilename . ': ' . $!,getlogger(__PACKAGE__));
        return 0;
    } else {
        $XLS->close();
    }

    #my $Formatter = Spreadsheet::ParseExcel::FmtUnicode->new(Unicode_Map => $SourceCharset);

    my $reader   = Excel::Reader::XLSX->new();
    my $workbook = $reader->read_file($SourceFilename); #->parse($XLS,$Formatter); #$SourceFilename

    my $SourceCharset = $workbook->{_reader}->encoding();
    $DestCharset = $SourceCharset unless $DestCharset;

    xls2csvinfo('reading ' . $SourceFilename . ' as ' . $SourceCharset,getlogger(__PACKAGE__));

    if ( !defined $workbook ) {
        xls2csverror($reader->error(),getlogger(__PACKAGE__));
        #die $parser->error(), ".\n";
        #$XLS->close();
        return 0;
    }

    #my $Book = Spreadsheet::ParseExcel::Workbook->Parse($XLS, $Formatter) or xls2csverror('can\'t read spreadsheet',getlogger(__PACKAGE__));

    my $sheet;
    if ($worksheet) {

        #my $test = $Book->GetContent();

    $sheet = $workbook->worksheet($worksheet);
    if (!defined $sheet) {
            xls2csverror('invalid spreadsheet',getlogger(__PACKAGE__));
            return 0;
        }
    #unless ($O{'q'})
    #{
    #   print qq|Converting the "$Sheet->{Name}" worksheet.\n|;
    #}
        xls2csvinfo('converting the ' . $sheet->name() . ' worksheet',getlogger(__PACKAGE__));
    } else {
        $sheet = $workbook->worksheet(0);
        if (@{$workbook->worksheets()} > 1) {
        #print qq|Multiple worksheets found. Will convert the "$Sheet->{Name}" worksheet.\n|;
            xls2csvinfo('multiple worksheets found, converting ' . $sheet->name(),getlogger(__PACKAGE__));
    }
    }

    unlink $DestFilename;
    local *CSV;
    if (not open(CSV,'>' . $DestFilename)) {
        fileerror('cannot open file ' . $DestFilename . ': ' . $!,getlogger(__PACKAGE__));
        #$XLS->close();
        return 0;
    }
    binmode CSV;

    my $csv = Text::CSV_XS->new({
            'quote_char'  => $quote_char,
            'escape_char' => $escape_char,
            'sep_char'    => $sep_char,
            'binary'      => 1,
    });

    my $Recoder;
    if ($DestCharset) {
    $Recoder = Locale::Recode->new(from => $SourceCharset, to => $DestCharset);
    }

    while ( my $row = $sheet->next_row() ) {

        foreach my $value ($row->values()) {
            $Recoder->recode($value);
        }

        my $status = $csv->combine($row->values());

        if (!defined $status) {
            xls2csvwarn('csv error: ' . $csv->error_input(),getlogger(__PACKAGE__));
        }

        if (defined $status) {
            if ($row->row_number() > 0) {
                print CSV $eol;
            }
            print CSV $csv->string();
            $csvlinecount++;
        }
    }

    close CSV;
    #$XLS->close;

    xls2csvinfo($csvlinecount . ' line(s) converted',getlogger(__PACKAGE__));

    return $csvlinecount;

}

1;
