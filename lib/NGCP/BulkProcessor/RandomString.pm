package NGCP::BulkProcessor::RandomString;
use strict;

## no critic

require Exporter;
our @ISA = qw(Exporter);
our @EXPORT_OK = qw(
    check_passwordstring
    createsalt
    createpassworddummy
    createtmpstring

    $passwordokmessage
    $passwordtooshortviolationmessage
    $passwordtoolongviolationmessage
    $passwordinvalidcharfoundviolationmessage
    $passwordcharacterminoccurenceviolationmessage
    $passwordcharactermaxoccurenceviolationmessage

    $smallletterscharacterclass
    $capitalletterscharacterclass
    $digitscharacterclass
    $umlautscharacterclass
    $altsymbolscharacterclass
    $symbolscharacterclass

    $characterclasses
);

our $maxpasswordlength = 30;
#our $maxpasswordfieldsize = 36;
our $minpasswordlength = 6;
our $saltlength = 8;
our $passworddummylength = 8;

our $smallletterscharacterclass = 1;
our $capitalletterscharacterclass = 2;
our $digitscharacterclass = 3;
our $umlautscharacterclass = 4;
our $altsymbolscharacterclass = 5;
our $symbolscharacterclass = 6;

our $characterclasses = { $smallletterscharacterclass   => ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'],
                          $capitalletterscharacterclass => ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'],
                          $digitscharacterclass         => ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],
                          $umlautscharacterclass        => ['�', '�', '�', '�', '�', '�', '�'],
                          $altsymbolscharacterclass     => ['�','�','@','~'],
                          $symbolscharacterclass        => ['^','�','!','"','�','$','%','&','/','{','(','[',']',')','}','=','?','\\','�','`','+','*','-','#','\'','-','_','.',':',';','|','<','>']
                        };

my $passworddummycharacterset = [$smallletterscharacterclass,$digitscharacterclass];
my $passwordcharacterset = [$smallletterscharacterclass,$capitalletterscharacterclass,$digitscharacterclass,$umlautscharacterclass,$altsymbolscharacterclass,$symbolscharacterclass];
my $saltcharacterset = [$smallletterscharacterclass,$capitalletterscharacterclass,$digitscharacterclass];
my $tmpcharacterset = [$capitalletterscharacterclass,$digitscharacterclass];

my $passworddummycharacterminoccurences = {
    $smallletterscharacterclass    => 0,
    $digitscharacterclass          => 0};
my $passworddummycharactermaxoccurences = {
    $smallletterscharacterclass    => $maxpasswordlength,
    $digitscharacterclass          => $maxpasswordlength};
my $passwordcharacterminoccurences = {
    $smallletterscharacterclass    => 0,
    $capitalletterscharacterclass  => 0,
    $digitscharacterclass          => 0,
    $umlautscharacterclass         => 0,
    $altsymbolscharacterclass      => 0,
    $symbolscharacterclass         => 0};
my $passwordcharactermaxoccurences = {
    $smallletterscharacterclass    => $maxpasswordlength,
    $capitalletterscharacterclass  => $maxpasswordlength,
    $digitscharacterclass          => $maxpasswordlength,
    $umlautscharacterclass         => $maxpasswordlength,
    $altsymbolscharacterclass      => $maxpasswordlength,
    $symbolscharacterclass         => $maxpasswordlength};
my $saltcharacterminoccurences = {
    $smallletterscharacterclass    => 2,
    $capitalletterscharacterclass  => 2,
    $digitscharacterclass          => 2};
my $saltcharactermaxoccurences = {
    $smallletterscharacterclass    => $saltlength,
    $capitalletterscharacterclass  => $saltlength,
    $digitscharacterclass          => $saltlength};
my $tmpcharacterminoccurences = {};
my $tmpcharactermaxoccurences = {};

our $passwordokmessage = 1;
our $passwordtooshortviolationmessage = -1;
our $passwordtoolongviolationmessage = -2;
our $passwordinvalidcharfoundviolationmessage = -3;
our $passwordcharacterminoccurenceviolationmessage = -4;
my $passwordcharacterminoccurenceviolationmessages = {
    $smallletterscharacterclass    => 'PasswordCharacterMinOccurenceViolation1Message',
    $capitalletterscharacterclass  => 'PasswordCharacterMinOccurenceViolation2Message',
    $digitscharacterclass          => 'PasswordCharacterMinOccurenceViolation3Message',
    $umlautscharacterclass         => 'PasswordCharacterMinOccurenceViolation4Message',
    $altsymbolscharacterclass      => 'PasswordCharacterMinOccurenceViolation5Message',
    $symbolscharacterclass         => 'PasswordCharacterMinOccurenceViolation6Message'};
our $passwordcharactermaxoccurenceviolationmessage = -5;
my $passwordcharactermaxoccurenceviolationmessages = {
    $smallletterscharacterclass    => 'PasswordCharacterMaxOccurenceViolation1Message',
    $capitalletterscharacterclass  => 'PasswordCharacterMaxOccurenceViolation2Message',
    $digitscharacterclass          => 'PasswordCharacterMaxOccurenceViolation3Message',
    $umlautscharacterclass         => 'PasswordCharacterMaxOccurenceViolation4Message',
    $altsymbolscharacterclass      => 'PasswordCharacterMaxOccurenceViolation5Message',
    $symbolscharacterclass         => 'PasswordCharacterMaxOccurenceViolation6Message'};
my $passwordviolationmessages = {
    $passwordokmessage                              => 'PasswordOKMessage',
    $passwordtooshortviolationmessage               => 'PasswordTooShortViolationMessage',
    $passwordtoolongviolationmessage                => 'PasswordTooLongViolationMessage',
    $passwordinvalidcharfoundviolationmessage       => 'PasswordInvalidCharFoundViolationMessage',
    $passwordcharacterminoccurenceviolationmessage  => '',
    $passwordcharactermaxoccurenceviolationmessage  => ''};

sub randstring {

  my ($lengthofstring,$characterclasses_ref,$characterset_ref,$minoccurences_ref,$maxoccurences_ref) = @_;

  my $output = '';

  if ($lengthofstring > 0) {

    my %classesusedcount = ();
    my %classesrequiredcount = ();
    my $classesrequiredcountsum = 0;

    my @characterset = @$characterset_ref;

    foreach my $characterclassid (@characterset) {

      if (exists $minoccurences_ref->{$characterclassid}) {
        $classesrequiredcount{$characterclassid} = $minoccurences_ref->{$characterclassid};
        $classesrequiredcountsum += $minoccurences_ref->{$characterclassid};
      } else {
        $classesrequiredcount{$characterclassid} = 0;
      }
      $classesusedcount{$characterclassid} = 0;

    }

    for (my $i = 0; $i < $lengthofstring; $i += 1) {

      my %availablerandcharacters = ();
      my @currentcharacterset = ();
      my $charactersleft = $lengthofstring - $i;

      foreach my $characterclassid (@characterset) {

        if ($classesrequiredcountsum >= $charactersleft) {
          if (exists $minoccurences_ref->{$characterclassid} and
              $classesrequiredcount{$characterclassid} > 0) {

            my @characters = @{$characterclasses_ref->{$characterclassid}};
            my $characterindex = int(rand($#characters + 1) + 1);
            $availablerandcharacters{$characterclassid} = $characters[$characterindex - 1];
            push @currentcharacterset,$characterclassid;

          }
        } else {
          if ((!exists $maxoccurences_ref->{$characterclassid}) or
              (exists $maxoccurences_ref->{$characterclassid} and
               $classesusedcount{$characterclassid} < $maxoccurences_ref->{$characterclassid})) {

            my @characters = @{$characterclasses_ref->{$characterclassid}};
            my $characterindex = int(rand($#characters + 1) + 1);
            $availablerandcharacters{$characterclassid} = $characters[$characterindex - 1];
            push @currentcharacterset,$characterclassid;

          }
        }
      }

      my $charactersetclassindex = int(rand(scalar @currentcharacterset) + 1);
      my $characterclassid = $currentcharacterset[$charactersetclassindex - 1];
      $classesrequiredcount{$characterclassid}--;
      if (exists $minoccurences_ref->{$characterclassid}) {
        $classesrequiredcountsum--;
      }

      $classesusedcount{$characterclassid}++;

      $output .= $availablerandcharacters{$characterclassid};

    }
  }

  return $output;

}

sub check_passwordstring {

    my ($password) = @_;

    if (length($password) < $minpasswordlength) {
        return ($passwordtooshortviolationmessage,
                $passwordviolationmessages->{$passwordtooshortviolationmessage});
    } elsif (length($password) > $maxpasswordlength) {
        return ($passwordtoolongviolationmessage,
                $passwordviolationmessages->{$passwordtoolongviolationmessage});
    }

    my $validcharcount = 0;

    foreach my $characterclassid (@$passwordcharacterset) {

        my @characters = @{$characterclasses->{$characterclassid}};
        my $occurencecount = 0;

        foreach my $character (@characters) {
            $occurencecount += _substringoccurence($password,$character);
        }

        $validcharcount += $occurencecount;

        if (exists $passwordcharacterminoccurences->{$characterclassid} and
            $occurencecount < $passwordcharacterminoccurences->{$characterclassid}) {
            return ($passwordcharacterminoccurenceviolationmessage,
                    $passwordcharacterminoccurenceviolationmessages->{$characterclassid});
      } elsif (exists $passwordcharactermaxoccurences->{$characterclassid} and
               $occurencecount > $passwordcharactermaxoccurences->{$characterclassid}) {
            return ($passwordcharactermaxoccurenceviolationmessage,
                    $passwordcharactermaxoccurenceviolationmessages->{$characterclassid});
      }

    }

    if ($validcharcount < length($password)) {
        return ($passwordinvalidcharfoundviolationmessage,
                $passwordviolationmessages->{$passwordinvalidcharfoundviolationmessage});
    }

    return ($passwordokmessage,
            $passwordviolationmessages->{$passwordokmessage});

}

sub _substringoccurence {
  my ($inputstring,$substring) = @_;
  my $result = 0;
  my $position = 0;
  my $posincrement = length($substring);
  if ($posincrement > 0) {
    do {
      $position = index($inputstring,$substring,$position);
      if ($position >= 0) {
        $result += 1;
        $position += $posincrement;
      }
    } while ($position >= 0);
  }
  return $result;
}

sub createsalt {

    return randstring($saltlength,
                      $characterclasses,
                      $saltcharacterset,
                      $saltcharacterminoccurences,
                      $saltcharactermaxoccurences);

}

sub createpassworddummy {

    return randstring($passworddummylength,
                      $characterclasses,
                      $passworddummycharacterset,
                      $passworddummycharacterminoccurences,
                      $passworddummycharactermaxoccurences);

}

sub createtmpstring {

    my $lengthofstring = shift;
    return randstring($lengthofstring,
                      $characterclasses,
                      $tmpcharacterset,
                      $tmpcharacterminoccurences,
                      $tmpcharactermaxoccurences);

}

1;
