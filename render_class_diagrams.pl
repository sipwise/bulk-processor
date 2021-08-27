use strict;

use File::Basename;
use Cwd;
use lib Cwd::abs_path(File::Basename::dirname(__FILE__));

use NGCP::BulkProcessor::Globals qw($system_version
    $system_abbreviation
    $system_instance_label
    $application_path
);

our $umldiagrampath = Cwd::abs_path(File::Basename::dirname(__FILE__)) . '/uml/';

our $uml_diagram_format = 'dot'; #'png'

use NGCP::BulkProcessor::Utils qw(
    getscriptpath
    cleanupdir
    makepath
    changemod
    getscriptpath
);

use NGCP::BulkProcessor::Array qw(contains);

use NGCP::BulkProcessor::Logging qw(
    getlogger
    scriptinfo
);
use NGCP::BulkProcessor::LogError qw(
    fileerror
    filewarn
    done
);

use File::Find;
use File::Basename;
use UML::Class::Simple;

my $graphviz_dot_path;
if ($^O eq 'MSWin32') {
    # IPC run3 used in UML::Class::Simple obviously ignores windows (vista 32)
    # PATH variable. so we specify the path to graphviz dot.exe manually:
    #$graphviz_dot_path = 'C:\Program Files\Graphviz2.26.3\bin\dot.exe';
    $graphviz_dot_path = 'C:\Program Files (x86)\Graphviz2.26.3\bin\dot.exe';
} else {
    # unix probably ok - leave UML::Class::Simple alone:
    $graphviz_dot_path = undef;  
}

my $logger = getlogger(getscriptpath());

my @dirs_to_skip = ();
push @dirs_to_skip,$application_path . "Dao/mr553/";
push @dirs_to_skip,$application_path . "Dao/mr457/";
push @dirs_to_skip,$application_path . "Dao/mr441/";
push @dirs_to_skip,$application_path . "Dao/mr341/";
push @dirs_to_skip,$application_path . "Dao/mr103/";
#push @dirs_to_skip,$application_path . "Dao/mr102/";
push @dirs_to_skip,$application_path . "Dao/mr38/";
push @dirs_to_skip,$application_path . "Dao/Trunk/accounting";
push @dirs_to_skip,$application_path . "Dao/Trunk/billing";
push @dirs_to_skip,$application_path . "Dao/Trunk/kamailio";

push @dirs_to_skip,$application_path . "Redis/mr65/";
push @dirs_to_skip,$application_path . "RestRequests/";
push @dirs_to_skip,$application_path . "Service/";
push @dirs_to_skip,$application_path . "RestConnectors/";

push @dirs_to_skip,$application_path . "Projects/t/";
push @dirs_to_skip,$application_path . "Projects/ETL/";
push @dirs_to_skip,$application_path . "Projects/Export/";
push @dirs_to_skip,$application_path . "Projects/Disaster/";
push @dirs_to_skip,$application_path . "Projects/Migration/";
push @dirs_to_skip,$application_path . "Projects/Massive/Generator/";
push @dirs_to_skip,$application_path . "Projects/Massive/RegistrationMonitoring/";

#push @dirs_to_skip,$application_path . "FileProcessors/";

my $public_only = 1;
my $inherited_methods = 0;

#my @perlfileextensions = ('.pm'); #('.pl','.pm');
my @perlfileextensions = ('.pm');
my $rperlextensions = join('|',map { local $_ = $_ ; $_ = quotemeta($_); $_; } @perlfileextensions);

my @parsedfiles = ();

my $mode = $ARGV[0];

eval {

    my $painter;
    my $outputfilepath;

    if ('cleanup' eq $mode) {
	cleanupdir($umldiagrampath,0,\&scriptinfo,\&filewarn,$logger);
    } elsif ('all' eq $mode) {
	find({ wanted => \&dir_names, follow => 1 }, $application_path);

	$outputfilepath = $umldiagrampath . '_' . $system_abbreviation . '_' . $system_instance_label . '.' . $uml_diagram_format;

	$painter = getpainter(\@parsedfiles);
    
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('dao_ngcp' eq $mode) {
	$outputfilepath = $umldiagrampath . '_dao_ngcp.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'SqlRecord.pm',
	    $application_path . 'Dao/Trunk/provisioning/voip_subscribers.pm',
	]);
    
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('dao_openser' eq $mode) {
	$outputfilepath = $umldiagrampath . '_dao_openser.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'SqlRecord.pm',
	    $application_path . 'Dao/mr102/openser/location.pm',
	    $application_path . 'Dao/mr102/openser/voicemail_users.pm',	
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('dao_regmon' eq $mode) {
	$outputfilepath = $umldiagrampath . '_dao_regmon.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'SqlRecord.pm',
	    $application_path . 'Projects/Massive/VodafoneRO/RegistrationMonitoring/Dao/Proc4SwDynamic.pm',
	    $application_path . 'Projects/Massive/VodafoneRO/RegistrationMonitoring/Dao/Proc4SwStatic.pm',
	    $application_path . 'Projects/Massive/VodafoneRO/RegistrationMonitoring/Dao/SwStatus.pm',
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('dao_cmtsmon' eq $mode) {    
	$outputfilepath = $umldiagrampath . '_dao_cmtsmon.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'SqlRecord.pm',
	    $application_path . 'Projects/Massive/VodafoneRO/CmtsMonitoring/Dao/SwInputDynamic.pm',
	    $application_path . 'Projects/Massive/VodafoneRO/CmtsMonitoring/Dao/SwOutput.pm',
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('sql_connectors' eq $mode) {    
	$outputfilepath = $umldiagrampath . '_sql_connectors.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'SqlConnector.pm',
	    $application_path . 'SqlProcessor.pm',
	    $application_path . 'SqlRecord.pm',
	    $application_path . 'SqlConnectors/MySQLDB.pm',
	    $application_path . 'SqlConnectors/OracleDB.pm',
	    $application_path . 'SqlConnectors/SQLiteDB.pm',
	    $application_path . 'SqlConnectors/CSVDB.pm',
	    $application_path . 'SqlConnectors/SQLServerDB.pm',
	    $application_path . 'SqlConnectors/PostgreSQLDB.pm',
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('no_sql_connectors' eq $mode) {    
	$outputfilepath = $umldiagrampath . '_no_sql_connectors.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'NoSqlConnector.pm',
	    $application_path . 'NoSqlConnectors/Redis.pm',
	    $application_path . 'NoSqlConnectors/RedisEntry.pm',
	    $application_path . 'NoSqlConnectors/RedisProcessor.pm',
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('redis' eq $mode) {    
	$outputfilepath = $umldiagrampath . '_redis.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'NoSqlConnectors/RedisEntry.pm',
	    $application_path . 'Redis/mr755/location/usrdom.pm',
	    $application_path . 'Redis/mr755/location/entry.pm',
	]);
	
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}    
    } elsif ('framework1' eq $mode) {
	$outputfilepath = $umldiagrampath . '_framework1.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'Array.pm',
	    $application_path . 'Calendar.pm',
	    $application_path . 'ConnectorPool.pm',
	    $application_path . 'DSPath.pm',
	    $application_path . 'DSSorter.pm',
	]);
    
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('framework3' eq $mode) {
	$outputfilepath = $umldiagrampath . '_framework3.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'FileProcessor.pm',
	    $application_path . 'Globals.pm',
	    $application_path . 'LoadConfig.pm',
	    $application_path . 'LogError.pm',
	    $application_path . 'Logging.pm',
	    $application_path . 'Mail.pm',
	]);
    
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    } elsif ('framework2' eq $mode) {
	$outputfilepath = $umldiagrampath . '_framework2.' . $uml_diagram_format;
	$painter = getpainter([
	    $application_path . 'RestConnector.pm',
	    $application_path . 'RestItem.pm',
	    $application_path . 'RestProcessor.pm',
	    $application_path . 'Serialization.pm',
	    $application_path . 'Table.pm',
	    $application_path . 'Utils.pm',
	]);
    
	if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
	    scriptinfo($outputfilepath . ' created',$logger);
	} else {
	    print STDERR "Could not create $outputfilepath.\n";
	}
    }
};
if ($@) {
    exit(1); 
} else {
    done(undef,undef,$logger);
    exit(0); 
}

sub dir_names {
    
    my $path = $File::Find::dir;
    $path .= '/' . $_ if ($_ ne '.' and $_ ne '..');
    if (-d $path) {
        my $dir = $path . '/';
        unless (scalar grep { index($dir, $_, 0) == 0; } @dirs_to_skip) {
            #print 'creating perldoc html files for source files in ' . $path . "\n";
            push @parsedfiles,createumldiagramsofdir($dir,$umldiagrampath . substr($dir,length($application_path)));
            #push @dirs_to_skip,$dir;
        }
    }
}

sub createumldiagramsofdir {
  
    my ($inputdir,$outputdir) = @_;
    makepath($outputdir);
    
    local *DOCDIR;
    if (not opendir(DOCDIR, $inputdir)) {
        fileerror('cannot opendir ' . $inputdir . ': ' . $!,$logger);
        return;
    }
    my @files = grep { /$rperlextensions$/ && -f $inputdir . $_} readdir(DOCDIR);
    closedir DOCDIR;
    
    my @inputfilepaths = ();
  
    foreach my $file (@files) {
        my $inputfilepath = $inputdir . $file;
        my ($inputfilename,$inputfiledir,$inputfilesuffix) = fileparse($inputfilepath, $rperlextensions);
        my $outputfilepath = $outputdir . $inputfilename . '.' . $uml_diagram_format; #'.png';
  #unlink $outputfilepath;

        my $painter = getpainter($inputfilepath); 

        # we can explicitly specify the image size
        #$painter->size(5, 3.6); # in inches

        # ...and change the default title background color:
        #$painter->node_color('#ffffff'); # defaults to '#f1e1f4'
    
        # only show public methods and properties
        #$painter->public_only(1);
    
        # hide all methods from parent classes
        #$painter->inherited_methods(0);
    
        if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
          changemod($outputfilepath);
          push @inputfilepaths,$inputfilepath;
          scriptinfo('uml diagram ' . $outputfilepath . ' created for source file ' . $inputfilepath,$logger);
        } else {
          # write message to STDERR make it look the same as perldoc
          print STDERR 'Could not create uml diagram for "' . $inputfilepath . "\".\n";
        }
    }

    if (scalar @inputfilepaths > 0) {
      
        my @dirparts = File::Spec->splitdir($inputdir);
        my $outputfilepath = $outputdir . $dirparts[$#dirparts - 1] . '.' . $uml_diagram_format; #'.png';  
        
        my $painter = getpainter(\@inputfilepaths);
    
        # we can explicitly specify the image size
        #$painter->size(5, 3.6); # in inches
    
        # ...and change the default title background color:
        #$painter->node_color('#ffffff'); # defaults to '#f1e1f4'
    
        # only show public methods and properties
        #$painter->public_only(1);
    
        # hide all methods from parent classes
        #$painter->inherited_methods(0);

        if (outputpainter($painter,$outputfilepath) and -e $outputfilepath) {
            scriptinfo('uml diagram ' . $outputfilepath . ' created for source dir ' . $inputdir,$logger);
        } else {
            # write message to STDERR make it look the same as perldoc
            print STDERR 'Could not create uml diagram for dir "' . $inputdir . "\".\n";
        }
    }
    
    return @inputfilepaths;
  
}

sub getpainter {
  
    my $inputfilepaths = shift;
  
    my @classes = classes_from_files($inputfilepaths);
    my $painter = UML::Class::Simple->new(\@classes);
    if ($graphviz_dot_path) {
        $painter->{dot_prog} = $graphviz_dot_path;
    }
    
    $painter->public_only($public_only);
    $painter->inherited_methods($inherited_methods);
    #$painter->size(8.3, 11.7);
    
    return $painter;
  
}

sub outputpainter {
  
  my ($painter,$outputfilepath) = @_;
    
  if (scalar @{$painter->as_dom()->{classes}} > 0) {
    if ($uml_diagram_format eq 'png') {
      $painter->as_png($outputfilepath);
    } elsif ($uml_diagram_format eq 'dot') {
      $painter->as_dot($outputfilepath);
    }
    return 1;
  } else {
    return 0;
  }
  
}
