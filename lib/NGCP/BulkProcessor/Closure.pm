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


use JavaScript::QuickJS qw();

use JSON qw();
use JSON::PP qw();

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

# get_env() lists the context variables and helpers, ie. all globals except
# the js builtins and _func:
my $JS_ENV = <<'JS';
globalThis._builtins = Object.getOwnPropertyNames(globalThis);
globalThis.get_env = function() {
    return Object.getOwnPropertyNames(globalThis).filter(function(k) {
        return _builtins.indexOf(k) < 0 && k.charAt(0) != '_';
    });
};
JS

my $ANON_CLASS = 'closure_anon';
sub closure_anon::TO_JSON {
    return _unbless(@_);
};

my %interpreter_cache = ();
my %stash = ();
my %js_exported_map = ();

# a QuickJS engine cannot be shared with a new thread (its context would be
# freed twice), so threads skip cloning it and create their own engines.
sub JavaScript::QuickJS::CLONE_SKIP { 1 }
sub CLONE {
    foreach my $code (keys %js_exported_map) {
        delete $interpreter_cache{$code};
        delete $js_exported_map{$code};
    }
}

sub _stash_get {
    my $k = shift;
    return $stash{$k} if $k;
}
sub _stash_set {
    my ($k,$v) = @_;
    $stash{$k} = $v if $k;
}

sub cleanup {

    clear_stash();
    foreach my $code (keys %interpreter_cache) {
        delete $interpreter_cache{$code};
        delete $js_exported_map{$code};
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
                        } elsif ($ANON_CLASS eq ref $context->{$_}) {
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
        eval {
            my $js = _get_js($code);
            $js->eval(_serialize_js_args($js,$context,$js_exported_map{$code})) if $context;
        };
        die("$self->{description}: " . $@) if $@;
    } else {
        die("unsupported expression langage");
    }

    return $self;

}

sub _get_js {

    my $code = shift;
    return $interpreter_cache{$code} if exists $interpreter_cache{$code};
    my $js = JavaScript::QuickJS->new();
    $js->eval($JS_ENV . "\nvar _func = " . $code . ';');
    $interpreter_cache{$code} = $js;
    $js_exported_map{$code} = {};
    # not marked as exported, so context functions can override them:
    _serialize_js_args($js,{
        to_json => sub {
            my ($obj,$pretty, $canonical) = @_;
            return _to_json($obj, $pretty, $canonical);
        },
        quotemeta => sub {
            my $s = shift;
            return quotemeta($s);
        },
        sprintf => sub {
            my ($f,@p) = @_;
            return sprintf($f, @p);
        },
        stash_get => sub {
            my $k = shift;
            return _stash_get($k);
        },
        stash_set => sub {
            my ($k,$v) = @_;
            _stash_set($k,$v);
        },
    },{});
    return $js;

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

sub _serialize_js_args {

    my ($js,$args,$js_env) = @_;
    my $sep;
    my @args;
    if ('HASH' eq ref $args and $js_env) {
        $sep = ";\n";
        @args = map { { k => $_, v => $args->{$_}, }; } _get_public_vars($args);
    } else {
        $sep = ",";
        @args = map { { k => undef, v => $_, }; } @$args;
    }
    return join ($sep,map {
        if ('CODE' eq ref $_->{v}) {
            if ($_->{k} and not $js_env->{$_->{k}}) {
                $js_env->{$_->{k}} = 1;
                my $sub = $_->{v};
                $js->set_globals($_->{k} => sub {
                    return _to_js_value(scalar $sub->(map { _unbox_js_value($_); } @_));
                });
            }
            ();
        } elsif (blessed $_->{v} and $_->{v}->isa('NGCP::BulkProcessor::SqlConnector')) {
            if ($_->{k} and not $js_env->{$_->{k}}) {
                $js_env->{$_->{k}} = 1;
                my $db = $_->{v};
                no strict 'refs'; ## no critic (ProhibitNoStrict)
                foreach my $k (keys %NGCP::BulkProcessor::SqlConnector::) { 
                    next unless substr($k,0,3) eq "db_";
                    if (exists &{"NGCP::BulkProcessor::SqlConnector::$k"}) { # check if symbol is method
                        $js->set_globals($k => sub {
                            return _to_js_value(scalar $db->$k(map { _unbox_js_value($_); } @_));
                        });
                    }
                }
            }
            ();
        } elsif (('ARRAY' eq ref $_->{v})
                 or ('HASH' eq ref $_->{v})
                 or ($ANON_CLASS eq ref $_->{v})) {
            if (not $_->{k}) {
                _to_json($_->{v});
            } elsif ($js_env->{$_->{k}}) {
                $_->{k} . ' = ' . _to_json($_->{v});
            } else {
                $js_env->{$_->{k}} = 1;
                'var ' . $_->{k} . ' = ' . _to_json($_->{v});
            }
        } elsif (('ARRAY' eq reftype($_->{v}))
                 or ('HASH' eq reftype($_->{v}))) {
            if (not $_->{k}) {
                _unbless_to_json($_->{v});
            } elsif ($js_env->{$_->{k}}) {
                $_->{k} . ' = ' . _unbless_to_json($_->{v});
            } else {
                $js_env->{$_->{k}} = 1;
                'var ' . $_->{k} . ' = ' . _unbless_to_json($_->{v});
            }            
        } elsif (ref $_->{v}) {
            warn((ref $_->{v}) . ' objects not available in javascript');
        } else {
            if (not $_->{k}) {
                "'" . _escape_js($_->{v}) . "'";
            } elsif ($js_env->{$_->{k}}) {
                $_->{k} . " = '" . _escape_js($_->{v}) . "'";
            } else { 
                $js_env->{$_->{k}} = 1;
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
            $v[0] = _unbless($v[0]) if ($ANON_CLASS eq ref $v[0]);
        };
        if ($@) {
            die("$self->{description}: " . $@);
        }
    } elsif ("perl" eq $self->{type}) {
        eval {
            @v = $interpreter_cache{$self->{source}}->(@_);
            $v[0] = _unbless($v[0]) if ($ANON_CLASS eq ref $v[0]);
        };
        if ($@) {
            die("$self->{description}: " . $@);
        }
    } elsif ("js" eq $self->{type}) {
        eval {
            my $js = _get_js($self->{source});
            my $updated_js_env = '';
            $updated_js_env = _serialize_js_args($js,$context,$js_exported_map{$self->{source}}) if $context;
            $updated_js_env .= ";\n" if length($updated_js_env);
            my $call;
            if (scalar @_) {
                $call = "_func(" . _serialize_js_args($js,[ @_ ],$js_exported_map{$self->{source}}) . ");";
            } else {
                $call = "_func();"
            }
            $v[0] = _unbox_js_value($js->eval($updated_js_env . $call));
        };
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

sub _unbox_js_value {

    my $v = shift;
    return $v unless defined $v; # keep null array elements and hash values
    if (JSON::PP::is_bool($v)) {
        $v = ($v ? 1 : 0);
    } elsif ('JavaScript::QuickJS::Date' eq ref $v) {
        $v = int($v->getTime() / 1000); # epoch seconds, as with JE
    } elsif ((ref $v) =~ /^JavaScript::QuickJS::/) {
        die('unsupported js value: ' . ref $v);
    } elsif ($ANON_CLASS eq ref $v) {
        $v = _unbless($v);
    }
    if ('ARRAY' eq ref $v) {
        return [ map { _unbox_js_value($_); } @$v ];
    } elsif ('HASH' eq ref $v) {
        return { map { $_ => _unbox_js_value($v->{$_}); } keys %$v };
    } else {
        return $v;
    }

}

sub _to_js_value {

    my $v = shift;
    return $v unless ref $v;
    if (JSON::PP::is_bool($v)) {
        return ($v ? 1 : 0);
    } elsif ('CODE' eq ref $v or (ref $v) =~ /^JavaScript::QuickJS::/) {
        return $v;
    } elsif ('HASH' eq reftype($v)) {
        return { map { $_ => _to_js_value($v->{$_}); } keys %$v };
    } elsif ('ARRAY' eq reftype($v)) {
        return [ map { _to_js_value($_); } @$v ];
    }
    return undef; ## no critic (ProhibitExplicitReturnUndef)

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

sub _unbless_to_json {

    my $obj = shift;
    return _to_json(_unbless($obj),@_);

}

1;