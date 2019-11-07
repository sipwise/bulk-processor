use strict;
use warnings;

use Module::Build qw();
my $builder = Module::Build->new(
    dist_name       => 'NGCP::BulkProcessor',
    dist_abstract     => 'Framework for parallel/distributed processing of record blocks',
    license           => 'GPL_3',
    dist_author       => 'Rene Krenn <rkrenn@sipwise.com>',
    dist_version_from => 'lib/NGCP/BulkProcessor/Globals.pm',
    perl_version_from => 'lib/NGCP/BulkProcessor/Globals.pm',

    meta_merge => {
        'meta-spec' => {
            version => 2,
        },
        prereqs => {
            test => {
                requires => {
                    'Module::Runtime'                       => 0,
                    'Test::More'                            => 0,
                    'Test::Unit::Procedural'                => 0,
                },
            },
            runtime => {
                requires => {
                    'Config::Any'                           => 0,
                    'DBD::CSV'                              => '0.26',
                    'DBD::SQLite'                           => '1.29',
                    'DBD::mysql'                            => '4.014',
                    'DBI'                                   => '1.608',
                    'Data::Dump'                            => 0,
                    'Data::Rmap'                            => 0,
                    'Data::UUID'                            => 0,
                    'Data::Validate::IP'                    => 0,
                    'Date::Calc'                            => 0,
                    'Date::Manip'                           => 0,
                    'DateTime'                              => 0,
                    'DateTime::Format::ISO8601'             => 0,
                    'DateTime::Format::Strptime'            => 0,
                    'DateTime::TimeZone'                    => 0,
                    'Digest::MD5'                           => 0,
                    'Email::MIME'                           => 0,
                    'Email::MIME::Attachment::Stripper'     => 0,
                    'Excel::Reader::XLSX'                   => 0,
                    'Gearman::Client'                       => 0,
                    'Gearman::Task'                         => 0,
                    'Gearman::Worker'                       => 0,
                    'HTML::Entities'                        => 0,
                    'HTML::PullParser'                      => 0,
                    'HTTP::Request'                         => 0,
                    'HTTP::Status'                          => 0,
                    'IO::Socket::SSL'                       => 0,
                    'IO::Uncompress::Unzip'                 => 0,
                    'JSON'                                  => 0,
                    'LWP::UserAgent'                        => 0,
                    'List::Util'                            => 0,
                    'Locale::Recode'                        => 0,
                    'Log::Log4Perl'                         => 0,
                    'MIME::Base64'                          => 0,
                    'MIME::Lite'                            => 0,
                    'MIME::Parser'                          => 0,
                    'Mail::IMAPClient'                      => 0,
                    'Marpa::R2'                             => 0,
                    'Net::Address::IP::Local'               => 0,
                    'Net::SMTP'                             => 0,
                    'PHP::Serialization'                    => 0,
                    'Spreadsheet::ParseExcel'               => 0,
                    'Spreadsheet::ParseExcel::FmtUnicode'   => 0,
                    'String::MkPasswd::mkpasswd'            => 0,
                    'Sys::CpuAffinity'                      => 0,
                    'Text::CSV_XS'                          => 0,
                    'Tie::IxHash'                           => 0,
                    'Time::HiRes'                           => 0,
                    'Time::Warp'                            => 0,
                    'URI'                                   => 0,
                    'URI::Find'                             => 0,
                    'UUID'                                  => 0,
                    'XML::Dumper'                           => '0.81',
                    'YAML::XS'                              => 0,
                },
                suggests => {
                    'DBD::ODBC'                             => '1.50',
                    'DBD::Oracle'                           => '1.21',
                    'DBD::Pg'                               => '2.17.2',
                },
            },
        },
    },
    add_to_cleanup => [ 'NGCP-BulkProcessor-*' ],
);
$builder->add_build_element('pl');
$builder->add_build_element('cfg');
$builder->add_build_element('yml');
$builder->create_build_script;
