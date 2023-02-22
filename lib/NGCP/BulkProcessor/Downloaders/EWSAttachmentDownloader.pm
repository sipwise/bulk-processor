package Downloaders::EWSAttachmentDownloader;
use warnings;
use strict;
use File::Basename;
use Cwd;
use lib File::Basename::dirname(__FILE__);
use lib Cwd::abs_path(File::Basename::dirname(__FILE__) . '/../');

use Logging qw(getlogger
			   attachmentdownloaderdebug
			   attachmentdownloaderinfo);
use LogError qw(fileerror
				attachmentdownloadererror
				attachmentdownloaderwarn);

use Utils qw(kbytes2gigs changemod);

use Office365::EWS::Client;
use AttachmentDownloader;

require Exporter;
our @ISA = qw(Exporter AttachmentDownloader);
our @EXPORT_OK = qw();

my $logger = getlogger(__PACKAGE__);

sub new {

  my $class = shift;
  my ($server,$mailbox,$tenant_id,$client_id,$client_secret,$foldername,$checkfilenamecode) = @_;
  my $self = AttachmentDownloader->new($class,$server,$mailbox,$tenant_id,$client_id,$client_secret,$foldername,$checkfilenamecode);
  attachmentdownloaderdebug('ews attachment downloader object created',$logger);
  return $self;

}

sub logout {
  my $self = shift;
  $self->{folder} = undef;
}

sub setup {

	my $self = shift;
	my ($server,$mailbox,$tenant_id,$client_id,$client_secret,$foldername,$checkfilenamecode) = @_;

	$self->logout();

	attachmentdownloaderdebug('ews attachment downloader setup - ' . $server,$logger);

	my $ews = Office365::EWS::Client->new({
		server         => $server, #'outlook.office.com',

		tenant_id => $tenant_id,
		client_id => $client_id,
		client_secret => $client_secret,

		use_negotiated_auth => 0,

	});

	my $entries = $ews->folders->retrieve({
		impersonate => $mailbox, 
	});
	$self->{impersonation} = {
		Impersonation => {
			ConnectingSID => {
				PrimarySmtpAddress => $mailbox,
			}
		},
	};
	$self->{request_version} = {
		RequestVersion => {
			Version => $ews->server_version,
		},
	};
	$self->{ews} = $ews;
	$self->{checkfilenamecode} = $checkfilenamecode;

	eval {
		while ($entries->has_next) {
			my $folder = $entries->next;
			#print Dumper($entries->next);
			#print $folder->DisplayName, "\n";
			my $subfolders = $folder->SubFolders;
			#print Dumper($subfolders);
			foreach my $folder (@$subfolders) {
				#my $subfolder = $entries->next;
				#print "    " . $folder->DisplayName . "\n";
				#print $folder->FolderId . "\n";
				if ($foldername eq $folder->DisplayName) { #"Posteingang"
					#$self->{folderId} = $folder->FolderId->{Id};
					$self->{folder} = $folder;
					attachmentdownloaderdebug('folder ' . $foldername . ' found',$logger);
					last;
				}
			}
			last if $self->{folder};
		}
	};
	if ($@) {
		attachmentdownloadererror($@,$logger);
	} else {
		attachmentdownloaderinfo('ews login successful',$logger);
	}

	if (not defined $self->{folder}) {
		attachmentdownloadererror('folder ' . $foldername . ' not found',$logger);
	}

}

sub download {

	my $self = shift;
	my $filedir = shift;

	my @files_saved = ();
	my $message_count = 0;
	my $found = 0;

	if (defined $self->{folder}) {

		attachmentdownloaderinfo('downloading messages from folder ' . $self->{folder}->DisplayName(),$logger);

		my $finditem_response = $self->{ews}->FindItem->(
			%{$self->{impersonation}},
			%{$self->{request_version}},
			#ItemShape => {
			#    BaseShape => Default,
			#    IncludeMimeContent => true,
			#},
			#ItemShape => {
			#	BaseShape => 'IdOnly',
			#},
			ItemShape => { BaseShape => 'AllProperties' },
			Traversal => 'Shallow',
			ParentFolderIds => {
				#cho_FolderId => {
				#   FolderID => {
				#       Id => $folder->FolderId->{Id},
				#   }
				#}
				cho_FolderId => [
					{
						FolderId => {
							#(exists $opts->{folderId} ? (
							#        Id => $folder->FolderId->{Id},
							#) : Id => "msgfolderroot",)
							Id => $self->{folder}->FolderId->{Id},
						},
					},
				],
			},
		);
		$self->{ews}->folders->_check_for_errors('FindItem', $finditem_response);

		my @finditem_messages = $self->{ews}->folders->_list_messages('FindItem',  $finditem_response);
		my $messages = $finditem_messages[0]->{FindItemResponseMessage}->{RootFolder}->{Items}->{cho_Item};

		foreach my $msg (@$messages) {
			#print "yyy\n";
			if ($msg->{Message}->{HasAttachments}) {
				#print Dumper($msg->{Message});
				#print "xxx\n";
				my $getitem_response = $self->{ews}->GetItem->(
					%{$self->{impersonation}},
					%{$self->{request_version}},
					ItemShape => {
						BaseShape => 'Default',
						IncludeMimeContent => 'false',
					},
					ItemIds => {
						cho_ItemId => {
							#  #$msg->{Message}->{ItemId},
							ItemId => {
								Id => $msg->{Message}->{ItemId}->{Id},
							},
						},
					},
				);
				#print "bbb";

				#print $getitem_response;
				#$self->{ews}->_check_for_errors('GetItem',  $getitem_response);
				#print "aaaa";

				my @getitem_messages = $self->{ews}->folders->_list_messages('GetItem',  $getitem_response);
				my $attachments = $getitem_messages[0]->{GetItemResponseMessage}->{Items}->{cho_Item}->[0]->{Message}->{Attachments}->{cho_ItemAttachment};
				#print Dumper($y[0]->{GetItemResponseMessage}->{Items}->{cho_Item}->[0]->{Message}->{Attachments}->{cho_ItemAttachment});
				#die();
				foreach my $attachment (@$attachments) {
					#print "$attachment->{FileAttachment}->{Name}\n";

					my $getattachment_response = $self->{ews}->GetAttachment->(
						%{$self->{impersonation}},
						%{$self->{request_version}},
						AttachmentIds => {
							cho_AttachmentId => {
								AttachmentId => {
									Id => $attachment->{FileAttachment}->{AttachmentId}->{Id},
								}
							}
						}
					);
					#$self->{ews}->_check_for_errors('GetAttachment',  $getattachment_response);
					my @getattachment_messages = $self->{ews}->folders->_list_messages('GetAttachment',  $getattachment_response);

					#print Dumper(@z);
					
					attachmentdownloaderinfo('processing message "' . $msg->{Message}->{Subject} . '"',$logger);

					$found |= $self->_process_attachments(
						#$getattachment_messages[0]->{GetItemResponseMessage}->{Items}->{cho_Item}->[0]->{Message}->{Subject}, #$message->header('Subject'),
						undef,
						$msg->{Message}->{Subject},
						$filedir,
						#$attachment->{FileAttachment}->{Name},
						#
						\@files_saved,
						{
							filename => $attachment->{FileAttachment}->{Name},
							payload => $getattachment_messages[0]->{GetAttachmentResponseMessage}->{Attachments}->{cho_ItemAttachment}->[0]->{FileAttachment}->{Content},
						}
					);
					$message_count++;

					last if $found;

				}

				last if $found;
			}
		}

	}

	if (scalar @files_saved == 0) {
		attachmentdownloaderwarn('ews attachment download complete - ' . $message_count . ' messages found, but no matching attachments saved',$logger);
	} else {
		attachmentdownloaderinfo('ews attachment download complete - ' . scalar @files_saved . ' files saved',$logger);
	}

	return \@files_saved;

}

1;