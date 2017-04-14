use Date::Parse; 

my $test_date='3/27/2017 01:43';

# print $test_date;
sub convert_date_to_timestamp{
	my $date_strip = "";
	# First try if the format is %m/%d/%Y %H:%M if not try the other format
	my $date_string = $_[0];
	if ($date_string !~ /^([0]{0,1}[1-9]|1[012])////([1-9]|([012][0-9])|(3[01]))////\d\d\d\d [012]{0,1}[0-9]:[0-6][0-9]$/) {
		print "Bad data string provided: $date_string\n";
		return 0;
	}
	$date_strip = str2time($date_string);
	return $date_strip;
}
my $result = convert_date_to_timestamp($test_date);
if($result != 0){
	print "Timestamp(seconds since the epoch for localtime): $result";
	}