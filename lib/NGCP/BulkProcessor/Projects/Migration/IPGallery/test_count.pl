
use strict;

my $buffersize = 100 * 1024;
my $default_encoding = 'UTF-8';

_get_linecount('/home/rkrenn/test/Features_Define.cfg',$default_encoding,\&breaklines);

exit;

sub breaklines {
    my ($buffer_ref) = @_;
    my $spearator = "\n";
    my $count = 0;
    my $last_record;
    my $records = [];
    foreach my $record (split(/$spearator(?=(?:\d+$spearator))/,$$buffer_ref)) {
        $count++;
        $last_record = $record;
        push(@$records,$record);
    }
    #if ($last_record =~ /$spearator\}\s*$/) {
    #    $$buffer_ref = '';
    #} else {
        $count--;
        $$buffer_ref = $last_record;
        pop @$records;
    #}
    return $count;
}

sub _get_linecount {

    my ($file,$encoding,$breaklines_code) = @_;

    #local $/ = $lineseparator;
    local *INPUTFILE_LINECOUNT;
    if (not open (INPUTFILE_LINECOUNT, '<:encoding(' . $encoding . ')', $file)) {
        print('get line count - cannot open file ' . $file . ': ' . $!);
        return undef;
    }
    binmode INPUTFILE_LINECOUNT;

    my $linecount = 0;

    my $buffer = '';
    my $chunk = undef;
    my $n = 0;
    while (defined ($n = read(INPUTFILE_LINECOUNT,$chunk,$buffersize)) && $n != 0) {
        $buffer .= $chunk;
        $linecount += &$breaklines_code(\$buffer);
    }
    if (not defined $n) {
        print('get line count - error reading file ' . $file . ': ' . $!);
        close(INPUTFILE_LINECOUNT);
        return undef;
    }
    close(INPUTFILE_LINECOUNT);

    return $linecount;

}
