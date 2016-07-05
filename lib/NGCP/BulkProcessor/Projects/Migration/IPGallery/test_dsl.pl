#!/usr/bin/perl
use warnings;
use strict;

## no critic

use Marpa::R2;
use Data::Dumper;


my $input = do { local $/; <DATA> };

my $dsl = << '__GRAMMAR__';
lexeme default = latm => 1

:start   ::= Records
:default ::= action => ::first

Records ::= Record+                                 action => list
Record     ::= SubscriberNumber '{' Options '}'
Options ::= Option+                                  action => list
Option ::= OptionName | OptionName '{' OptionValues '}'
OptionValues ::= OptionValue+                           action => list

SubscriberNumber ~ [0-9]+
OptionName     ~ [-a-zA-Z_0-9]+
OptionValue     ~ [-a-zA-Z_0-9]+
whitespace ~ [\s]+
:discard   ~ whitespace
__GRAMMAR__

sub hash {
    print "hash";
    +{ $_[1] => $_[3] }
    }

sub pair {
    print "pair";
          +{ $_[1] => $_[2] }
          }

sub second {
    print "second";
            [ @_[ 2 .. $#_-1 ] ]
            }

sub list {
    shift;
    print "list";
    \@_
    }

my $grammar = Marpa::R2::Scanless::G->new( { source => \$dsl } );
my $recce = Marpa::R2::Scanless::R->new(
    { grammar => $grammar, semantics_package => 'main' } );
#my $input = '42 * 1 + 7';
$recce->read( \$input );

my $value_ref = $recce->value;
my $value = $value_ref ? ${$value_ref} : 'No Parse';

print Dumper $value;

#my $parser = 'Marpa::R2::Scanless::G'->new({ source => \$grammar });

#print Dumper $parser->parse(\$input, 'main', { trace_terminals => 1 });

__DATA__
35627883323
{
	RegisteredIC
	{
		Selective_Call_Waiting
		Selective_Ring
		Ring_By_DayTime
		Ring_By_Call_Origin
		ReAnswer
		Selective_CW_Ring
		Ic_Selective_Barring
		Ic_Day_Time_Barring
		Ic_Date_Barring
		Ic_No_Answer
		Ic_Default_Ring
		Malicious
		Display_Calling_Party_CLI
		Call_Waiting_for_all_calls
		Cancel_Call_Waiting
		Hunting
		Leading_Number
		Block_Anonymous_Call
		Do_Not_Disturb
		Restrict_Automatic_Recall
		Restrict_Automatic_CallBack
		Ic_Cancel_All_Forwards
		Ic_On-Line_Malicious
		Ic_Barring_Pattern
	}
	RegisteredOG
	{
		Speed_Dial_one_Digit
		Speed_Dial_two_Digits
		Save_Dialed_Number
		Og_Selective_Barring
		Og_Day_Time_Barring
		Og_Date_Barring
		Og_Display_Name
		Feature_Keys
		Pre_Paid
		Metering
		Block_CLI
		Confidential_Number
		Automatic_CallBack
		Automatic_Recall
		Force_CLI
		Last_Number_Redial
		Og_Three_Way_Calling
		Og_Hold
		Og_Barring_Pattern
	}
	Log_Malicious_Calls
	Display_Calling_Party_CLI
	Cancel_Call_Waiting
	On_Line_Malicious
	Block_CLI
	Automatic_CallBack
	Automatic_Recall
	Force_CLI
	Last_Number_Redial
	Hold
	Default_Ring
	{
		1
	}
	Display_Name
	{
		O
	}
}
35627464746
{
	RegisteredIC
	{
		Selective_Call_Waiting
		Selective_Ring
		Ring_By_DayTime
		Ring_By_Call_Origin
		ReAnswer
		Forward_All_Calls
		Forward_On_Busy
		Forward_on_No_Answer
		Forward_Unavailable
		Selective_CW_Ring
		Ic_Selective_Barring
		Ic_Day_Time_Barring
		Ic_Date_Barring
		Ic_No_Answer
		Ic_Default_Ring
		Malicious
		Display_Calling_Party_CLI
		Call_Waiting_for_all_calls
		Cancel_Call_Waiting
		Hunting
		Leading_Number
		Block_Anonymous_Call
		Do_Not_Disturb
		Restrict_Automatic_Recall
		Restrict_Automatic_CallBack
		Ic_Cancel_All_Forwards
		Ic_On-Line_Malicious
		Ic_Barring_Pattern
	}
	RegisteredOG
	{
		Speed_Dial_one_Digit
		Speed_Dial_two_Digits
		Save_Dialed_Number
		Og_Selective_Barring
		Og_Day_Time_Barring
		Og_Date_Barring
		Og_Display_Name
		Og_Web_Access
		Feature_Keys
		Pre_Paid
		Metering
		Block_CLI
		Confidential_Number
		Automatic_CallBack
		Automatic_Recall
		Force_CLI
		Last_Number_Redial
		Og_Three_Way_Calling
		Og_Hold
		Og_Barring_Pattern
	}
	Log_Malicious_Calls
	Display_Calling_Party_CLI
	Cancel_Call_Waiting
	On_Line_Malicious
	Block_CLI
	Automatic_CallBack
	Automatic_Recall
	Force_CLI
	Last_Number_Redial
	Hold
	Default_Ring
	{
		1
	}
	Web_Password
	{
		27464746
	}
	Display_Name
	{
		27464746
	}
}
