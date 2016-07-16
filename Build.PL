use Module::Build qw();
my $builder = Module::Build->new(
    dist_name       => 'NGCP::BulkProcessor',
    dist_abstract     => 'Framework for parallel/distributed processing of record blocks',
    license           => 'GPL_3',
    dist_author       => 'Rene Krenn <rkrenn@sipwise.com>',
    dist_version_from => 'lib/NGCP/BulkProcessor/Globals.pm',
    perl_version_from => 'lib/NGCP/BulkProcessor/Globals.pm',
    requires          => {
        'Archive::Zip'                                          => 0,
        'OLE::Storage_Lite'                                     => 0,
        'XML::LibXML::Reader'                                   => 0,

        'Email::MIME'                                           => 0,
        'Email::MIME::Attachment::Stripper'                     => 0,

        'URI::Find'                                             => 0,
        'LWP::UserAgent'                                        => 0,
        'HTTP::Request'                                         => 0,

        'DateTime'                                              => 0,
        'Time::HiRes'                                           => 0,
        'Time::Warp'                                            => 0,
        'DateTime::TimeZone'                                    => 0,
        'DateTime::Format::Strptime'                            => 0,
        'DateTime::Format::ISO8601'                             => 0,

        'Tie::IxHash'                                           => 0,

        'URI::Escape'                                           => 0,
        'String::MkPasswd::mkpasswd'                            => 0,
        #'List::Util'                                            => 0,

        'Config::Any'                                           => 0,
        'YAML::Tiny'                                            => 0,
        'Log::Log4Perl'                                         => 0,
        'MIME::Base64'                                          => 0,
        'MIME::Lite'                                            => 0,
        'Net::SMTP'                                             => 0,
        'JSON::XS'                                              => 0,
        'Data::Dump'                                            => 0,
        'YAML::XS'                                              => 0,
        'XML::Dumper'                                           => '0.81',
        'PHP::Serialization'                                    => 0,
        #'Gearman::Worker'                                       => 0,
        'Gearman::Client'                                       => 0,
        #'Gearman::Task'                                         => 0,
        'Digest::MD5'                                           => 0,
        'Data::UUID'                                            => 0,
        'UUID'                                                  => 0,
        'Net::Address::IP::Local'                               => 0,
        'Date::Manip'                                           => 0,
        'Date::Calc'                                            => 0,
        #Sys::CpuAffinity
        'Marpa::R2'                                             => 0,
        'Data::Dumper::Concise'                                 => 0,
        'IO::Socket::SSL'                                       => 0,
        'Mail::IMAPClient'                                      => 0,
        'DBI'                                                   => '1.608',
        'DBD::CSV'                                              => '0.26',
        'Locale::Recode'                                        => 0,
        'Spreadsheet::ParseExcel'                               => 0,
        'Spreadsheet::ParseExcel::FmtUnicode'                   => 0,
        'Text::CSV_XS'                                          => 0,
        'MIME::Parser'                                          => 0,
        'HTML::Entities'                                        => 0,
        'IO::Uncompress::Unzip'                                 => 0,
        'DBD::mysql'                                            => '4.014',
        #'DBD::Oracle'                                           => '1.21',
        #'DBD::Pg'                                               => '2.17.2',
        #'DBD::ODBC'                                             => '1.50',
        'DBD::SQLite'                                           => '1.29',
    },
    test_requires => {
        'Module::Runtime'                                       => 0,
        'Test::Unit::Procedural'                                => 0,
    },
    add_to_cleanup => ['NGCP-BulkProcessor-*', 'Excel-Reader-*'],
);
$builder->add_build_element('pl');
$builder->add_build_element('cfg');
$builder->add_build_element('yml');
$builder->create_build_script;
