package NGCP::BulkProcessor::Closure;
use strict;

use warnings;

no warnings 'uninitialized'; ## no critic (ProhibitNoWarnings)

use Scalar::Util qw(blessed reftype);
use Eval::Closure qw(eval_closure);
my $eval_closure_make_lexical_assignment = sub {
    my ($key, $index, $alias) = @_;
    my $sigil = substr($key, 0, 1);
    my $name = substr($key, 1);
    if (Eval::Closure::HAS_LEXICAL_SUBS && $sigil eq '&') {
        my $tmpname = '$__' . $name . '__' . $index;
        return 'use feature "lexical_subs"; '
             . 'no warnings "experimental::lexical_subs"; '
             . 'my ' . $tmpname . ' = $_[' . $index . ']; '
             . 'my sub ' . $name . ' { goto ' . $tmpname . ' }';
    }
    if ($alias) {
        return 'my ' . $key . ';';
    }
    else {
        return 'my ' . $key . ' = ' . '$_[' . $index . '];';
        #return 'my ' . $key . ' = ' . $sigil . '{$_[' . $index . ']};';
    }
};
my $eval_closure_validate_env = sub {
    my ($env) = @_;

    croak("The 'environment' parameter must be a hashref")
        unless reftype($env) eq 'HASH';

    for my $var (keys %$env) {
        if (Eval::Closure::HAS_LEXICAL_SUBS) {
            croak("Environment key '$var' should start with \@, \%, \$, or \&")
                if index('$@%&', substr($var, 0, 1)) < 0;
        }
        else {
            croak("Environment key '$var' should start with \@, \%, or \$")
                if index('$@%', substr($var, 0, 1)) < 0;
        }
        #croak("Environment values must be references, not $env->{$var}")
        #    unless ref($env->{$var});
    }
};


#use JE::Destroyer qw();
use JE qw();

{
    no warnings 'redefine'; ## no critic (ProhibitNoWarnings)
    *JE::Object::evall = sub { 
        no warnings; ## no critic (ProhibitNoWarnings)
        my $global = shift;
        my $v = shift;
        my $r = eval 'local *_;' . $v; ## no critic (ProhibitStringyEval)
        if ($@) {
            my $e = $@;
            $r = eval "local *_;'$v'"; ## no critic (ProhibitStringyEval)
            if ($@) {
                die;
            }
        }
        $r;
    };
}

use JSON qw();

use YAML::Types;
{
    no warnings 'redefine'; ## no critic (ProhibitNoWarnings)
    *YAML::Type::code::yaml_load = sub {
        my $self = shift;
        my ($node, $class, $loader) = @_;
        if ($loader->load_code) {
            $node = "sub $node" unless $node =~ /^\s*sub/; #upstream backward compat
            my $code = eval "package yamlmain; no strict 'vars'; $node"; ## no critic (ProhibitStringyEval)
            if ($@) {
                die ($@);
                #$loader->warn('YAML_LOAD_WARN_PARSE_CODE', $@);
                #return sub {};
            }
            else {
                CORE::bless $code, $class if ($class and $YAML::LoadBlessed);
                return $code;
            }
        }
        else {
            return CORE::bless sub {}, $class if ($class and $YAML::LoadBlessed);
            return sub {};
        }
    };
}

use NGCP::BulkProcessor::SqlConnector qw();

use NGCP::BulkProcessor::Array qw(array_to_map);

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    closure
    cleanup
    is_code
    clear_stash
);

(my $DISABLED_CORE_FUNCTION_MAP, undef, undef) = array_to_map([ qw(
    binmode close closedir dbmclose dbmopen eof fileno flock format getc read
    readdir rewinddir say seek seekdir select syscall sysread sysseek
    syswrite tell telldir truncate write print printf

    chdir chmod chown chroot fcntl glob ioctl link lstat mkdir open opendir readlink
    rename rmdir stat symlink sysopen umask unlink utime

    alarm exec fork getpgrp getppid getpriority kill pipe setpgrp setpriority sleep
    system times wait waitpid

    accept bind connect getpeername getsockname getsockopt listen recv send setsockopt
    shutdown socket socketpair

    msgctl msgget msgrcv msgsnd semctl semget semop shmctl shmget shmread shmwrite

    endgrent endhostent endnetent endpwent getgrent getgrgid getgrnam getlogin getpwent
    getpwnam getpwuid setgrent setpwent

    endprotoent endservent gethostbyaddr gethostbyname gethostent getnetbyaddr
    getnetbyname getnetent getprotobyname getprotobynumber getprotoent getservbyname
    getservbyport getservent sethostent setnetent setprotoent setservent

    exit goto
)], sub { return shift; }, sub { return 1; }, 'last');

my @DISABLED_CORE_FUNCTIONS = grep { $DISABLED_CORE_FUNCTION_MAP->{$_}; } keys %$DISABLED_CORE_FUNCTION_MAP;

my $PERL_ENV = 'use subs qw(' . join(' ', @DISABLED_CORE_FUNCTIONS) . ");\n";
foreach my $f (@DISABLED_CORE_FUNCTIONS) {
    $PERL_ENV .= 'sub ' . $f . " { die('$f called'); }\n";
}

my $JS_ENV = '';

my $JE_ANON_CLASS = 'je_anon';
sub je_anon::TO_JSON {
    return _unbless(@_);
};

my %interpreter_cache = ();
my %stash = ();
my %je_exported_map = ();

sub _stash_get {
    my $k = shift;
    return $stash{$k} if $k;
}
sub _stash_set {
    my ($k,$v) = @_;
    $stash{$k} = $v if $k;
}

sub cleanup {

    eval {
        #no warnings 'deprecated';
        require JE::Destroyer;
        JE::Destroyer->import();
        1;
    } or do {
        return;
    };
    clear_stash();
    foreach my $code (keys %interpreter_cache) {
        JE::Destroyer::destroy($interpreter_cache{$code}) if 'JE' eq ref $interpreter_cache{$code}; # break circular refs
        delete $interpreter_cache{$code};
        delete $je_exported_map{$code};
    }

}

sub clear_stash {

    %stash = ();

}

sub new {

    my $class = shift;
    my $self = bless {}, $class;

    my ($code,$context,$description) = @_;

    $self->{description} = $description;
    if ('CODE' eq ref $code) {
        $self->{description} //= 'coderef';
        $self->{type} = "coderef";
        $self->{exported_map} = ();
        foreach my $key (_get_public_vars($context = {
                    get_env => sub {
                        return _filter_perl_env_symbols(keys %yamlmain::);
                    },
                    to_json => \&_unbless_to_json,
                    stash_get => \&_stash_get,
                    stash_set => \&_stash_set,
                    %{$context // {}},
                })) {
            _register_closure_var($key,$context->{$key});
            $self->{exported_map}->{$key} = 1;
        }
        $self->{code} = $code;
    } elsif ($code =~ /^\s*sub/) { #perl
        $self->{source} = $code;
        $self->{description} //= 'perl function';
        $self->{type} = "perl";
        unless (exists $interpreter_cache{$code}) {
            local *Eval::Closure::_make_lexical_assignment = $eval_closure_make_lexical_assignment;
            local *Eval::Closure::_validate_env = $eval_closure_validate_env;
            my @exported = ();
            eval {
                $interpreter_cache{$code} = eval_closure(
                    source      => ($PERL_ENV . $code),
                    environment => {
                        map { if ('ARRAY' eq ref $context->{$_}) {
                            push(@exported,$_);
                            ('$' . $_) => $context->{$_};
                        } elsif ('HASH' eq ref $context->{$_}) {
                            push(@exported,$_);
                            ('$' . $_) => $context->{$_};
                        } elsif ($JE_ANON_CLASS eq ref $context->{$_}) {
                            push(@exported,$_);
                            ('$' . $_) => _unbless($context->{$_});
                        } elsif ('CODE' eq ref $context->{$_}) {
                            push(@exported,$_);
                            ('&' . $_) => $context->{$_};
                        } elsif (ref $context->{$_}) {
                            push(@exported,$_);
                            ('$' . $_) => $context->{$_};
                        } else {
                            push(@exported,$_);
                            ('$' . $_) => $context->{$_};
                        } } _get_public_vars($context = {
                            get_env => sub {
                                no strict "refs"; ## no critic (ProhibitNoStrict)
                                return (@exported,_filter_perl_env_symbols(keys %{caller() .'::'}));
                            },
                            to_json => \&_unbless_to_json,
                            stash_get => \&_stash_get,
                            stash_set => \&_stash_set,
                            %{$context // {}},
                        })
                    },
                    terse_error => 1,
                    description => $self->{description},
                    alias => 0,
                );
            };
            if ($@) {
                die("$self->{description}: " . $@);
            }
        }
    } elsif ($code =~ /^\s*function/) { #javascript
        $self->{source} = $code;
        $self->{description} //= 'javascript function';
        $self->{type} = "js";
        my $je;
        if (exists $interpreter_cache{$code}) {
            $je = $interpreter_cache{$code};
        } else {
            $je_exported_map{$code} = {};
            $je = JE->new();
            $je->eval($JS_ENV . "\nvar _func = " . $code . ';');
            $interpreter_cache{$code} = $je;
        }
        $je->eval(_serialize_je_args($je,{
            get_env => sub {
                return [ _filter_js_env_symbols(keys %$je) ];
            },
            to_json => sub {
                my ($obj,$pretty, $canonical) = @_;
                return _to_json(_unbox_je_value($obj), _unbox_je_value($pretty), _unbox_je_value($canonical));
            },
            quotemeta => sub {
                my $s = shift;
                return quotemeta(_unbox_je_value($s));
            },
            sprintf => sub { 
                my ($f,@p) = @_;
                return sprintf(_unbox_je_value($f), map {
                    _unbox_je_value($_);
                } @p);
            },
            stash_get => sub {
                my $k = shift;
                return _stash_get(_unbox_je_value($k));
            },
            stash_set => sub {
                my ($k,$v) = @_;
                _stash_set(_unbox_je_value($k),_unbox_je_value($v));
            },
            %{$context // {}},
        },$je_exported_map{$code}));
        die("$self->{description}: " . $@) if $@;
    } else {
        die("unsupported expression langage");
    }

    return $self;

}

sub _register_closure_var {

    my ($key,$value) = @_;
    # modified globally?
    no strict "refs"; ## no critic (ProhibitNoStrict)
    if ('CODE' eq ref $value) {
        no warnings 'redefine'; ## no critic (ProhibitNoWarnings)
        *{"yamlmain::$key"} = $value;
    } else {
        ${"yamlmain::$key"} = $value;
    }

}

sub _get_public_vars {

    my $args = shift;
    return grep { substr($_,0,1) ne '_'; } keys %$args;

}

sub _serialize_je_args {

    my ($je,$args,$je_env) = @_;
    my $sep;
    my @args;
    if ('HASH' eq ref $args and $je_env) {
        $sep = ";\n";
        @args = map { { k => $_, v => $args->{$_}, }; } _get_public_vars($args);
    } else {
        $sep = ",";
        @args = map { { k => undef, v => $_, }; } @$args;
    }
    return join ($sep,map {
        if ('CODE' eq ref $_->{v}) {
            if ($_->{k} and not $je_env->{$_->{k}}) {
                $je_env->{$_->{k}} = 1;
                my $sub = $_->{v};
                $je->new_function($_->{k} => sub {
                    return $sub->(map { _unbox_je_value($_); } @_);
                });
            }
            ();
        } elsif (blessed $_->{v} and $_->{v}->isa('NGCP::BulkProcessor::SqlConnector')) {
            if ($_->{k} and not $je_env->{$_->{k}}) {
                $je_env->{$_->{k}} = 1;
                my $db = $_->{v};
                no strict 'refs'; ## no critic (ProhibitNoStrict)
                foreach my $k (keys %NGCP::BulkProcessor::SqlConnector::) { 
                    next unless substr($k,0,3) eq "db_";
                    if (exists &{"NGCP::BulkProcessor::SqlConnector::$k"}) { # check if symbol is method
                        $je->new_function($k => sub {
                            return $db->$k(map { _unbox_je_value($_); } @_);
                        });
                    }
                }
            }
            ();
        } elsif (('ARRAY' eq ref $_->{v})
                 or ('HASH' eq ref $_->{v})
                 or ($JE_ANON_CLASS eq ref $_->{v})) {
            if (not $_->{k}) {
                _to_json($_->{v});
            } elsif ($je_env->{$_->{k}}) {
                $_->{k} . ' = ' . _to_json($_->{v});
            } else {
                $je_env->{$_->{k}} = 1;
                'var ' . $_->{k} . ' = ' . _to_json($_->{v});
            }
        } elsif (('ARRAY' eq reftype($_->{v}))
                 or ('HASH' eq reftype($_->{v}))) {
            if (not $_->{k}) {
                _unbless_to_json($_->{v});
            } elsif ($je_env->{$_->{k}}) {
                $_->{k} . ' = ' . _unbless_to_json($_->{v});
            } else {
                $je_env->{$_->{k}} = 1;
                'var ' . $_->{k} . ' = ' . _unbless_to_json($_->{v});
            }            
        } elsif (ref $_->{v}) {
            warn((ref $_->{v}) . ' objects not available in javascript');
        } else {
            if (not $_->{k}) {
                "'" . _escape_js($_->{v}) . "'";
            } elsif ($je_env->{$_->{k}}) {
                $_->{k} . " = '" . _escape_js($_->{v}) . "'";
            } else { 
                $je_env->{$_->{k}} = 1;
                'var ' . $_->{k} . " = '" . _escape_js($_->{v}) . "'";
            }
        }
    } @args);

}

sub calc {

    my $self = shift;
    my $context = shift;
    my @v;
    if ("coderef" eq $self->{type}) {
        foreach my $key (_get_public_vars($context)) {
            unless ($self->{exported_map}->{$key}) {
                _register_closure_var($key,$context->{$key});
                $self->{exported_map}->{$key} = 1;
            }
        }
        eval {
            @v = $self->{code}->(@_);
            $v[0] = _unbless($v[0]) if ($JE_ANON_CLASS eq ref $v[0]);
        };
        if ($@) {
            die("$self->{description}: " . $@);
        }
    } elsif ("perl" eq $self->{type}) {
        @v = $interpreter_cache{$self->{source}}->(@_);
        $v[0] = _unbless($v[0]) if ($JE_ANON_CLASS eq ref $v[0]);
        if ($@) {
            die("$self->{description}: " . $@);
        }
    } elsif ("js" eq $self->{type}) {
        my $je = $interpreter_cache{$self->{source}};
        my $updated_je_env = '';
        $updated_je_env = _serialize_je_args($je,$context,$je_exported_map{$self->{source}}) if $context;
        $updated_je_env .= ";\n" if length($updated_je_env);
        my $call;
        if (scalar @_) {
            $call = "_func(" . _serialize_je_args($je,[ @_ ],$je_exported_map{$self->{source}}) . ");";
        } else {
            $call = "_func();"
        }
        $v[0] = _unbox_je_value($interpreter_cache{$self->{source}}->eval($updated_je_env . $call));
        if ($@) {
            die("$self->{description}: " . $@);
        }
    }
    
    return @v if wantarray;
    return $v[0];

}

sub is_code {

    my $code = shift;
    return unless defined $code;
    if ('CODE' eq ref $code) {
        return 1;
    } elsif (not ref $code) {
        if ($code =~ /^\s*function/) {
            return 1;
        } elsif ($code =~ /^\s*sub/) {
            return 1;
        }
    }
    return 0;

}

sub _unbox_je_value {

    my $v = shift;
    return undef unless defined $v; ## no critic (ProhibitExplicitReturnUndef)
    if ((ref $v) =~ /^JE::/) {
        $v = $v->value;
    } elsif ($JE_ANON_CLASS eq ref $v) {
        $v = _unbless($v);
    }
    if ('ARRAY' eq ref $v) {
        return [ map { _unbox_je_value($_); } @$v ];
    } elsif ('HASH' eq ref $v) {
        return { map { $_ => _unbox_je_value($v->{$_}); } keys %$v };
    } else {
        return $v;
    }

}

sub _unbless {

    my $obj = shift;
    if ('HASH' eq reftype($obj)) {
        return { map { $_ => _unbless($obj->{$_}); } keys %$obj };
    } elsif ('ARRAY' eq reftype($obj)) {
        return [ map { _unbless($_); } @$obj ];
    } else {
        return $obj;
    }

};

sub _escape_js {

    my $str = shift // '';
    my $quote_char = shift;
    $quote_char //= "'";
    $str =~ s/\\/\\\\/g;
    $str =~ s/$quote_char/\\$quote_char/g;
    return $str;

}

sub _to_json {

    my ($obj,$pretty,$canonical) = @_;
    return JSON::to_json($obj, {
        allow_nonref => 1, allow_blessed => 1, allow_unknown => 1,
        convert_blessed => 1, pretty => $pretty, canonical => $canonical, });

}

sub _filter_perl_env_symbols {

    return grep {
            $_ !~ /^__ANON__/
        and $_ !~ /^BEGIN/
        and not (exists $DISABLED_CORE_FUNCTION_MAP->{$_} and $DISABLED_CORE_FUNCTION_MAP->{$_})
    ; } @_;

}

sub _filter_js_env_symbols {

    return grep {
            $_ !~ /^_func/
    ; } @_;

}

sub _unbless_to_json {

    my $obj = shift;
    return _to_json(_unbless($obj),@_);

}

1;