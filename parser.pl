#!/usr/bin/env perl

use warnings;
use strict;

use DBI;
use Data::Dumper;
use Getopt::Long;

my ($file, $user_name, $password, $db_name, $db_host, $db_port) = (undef, undef, undef, undef, undef, undef);

GetOptions(
    'file|f=s'       => \$file,
    'user_name|u=s'  => \$user_name,
    'password|p=s'   => \$password,
    'db_name|d=s'    => \$db_name,
    'db_host|h=s'    => \$db_host,
    'db_port|port=i' => \$db_port,
);

my $is_params_ok = check_params($file, $user_name, $password, $db_name, $db_port);
if (!$is_params_ok) {
    print_usage();
}

my $datasource = "dbi:Pg:dbname=$db_name;host=$db_host;port=$db_port";
my $dbh = DBI->connect_cached($datasource, $user_name, $password, { AutoCommit => 0, RaiseError => 1 }) or die $DBI::errstr;
$dbh->do('TRUNCATE log; TRUNCATE message;');

my $query_message = 'INSERT INTO message (created, int_id, str, id) VALUES(?, ?, ?, ?)';
my $query_log = 'INSERT INTO log (created, int_id, str, address) VALUES(?, ?, ?, ?)';

my $sth_message = $dbh->prepare($query_message);
my $sth_log = $dbh->prepare($query_log);

open(my $fh, '<', $file) or die $!;

while (my $line = <$fh>) {
    $line =~ s!^\s+|\s+$|!!g;
    my ($date, $time) = $line =~ m!^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})!;
    my ($int_id) = $line =~ m!^$date\s+$time\s+(\w{6}-\w{6}-\w{2})!;
    if ($int_id) {
        my ($str) = $line =~ m!^$date\s+$time\s+(.*)!;
        my ($flag) = $line =~ m!$int_id\s+(<=|=>|->|\*\*|==)!;
        my ($address) = $line =~ m!(?:<=|=>|->|\*\*|==).*?[<]?(\S+\@\S+?\.\w+)[>]?\s!;
        my ($id) = $line =~ m!\s(id=\S+)!;
        my $is_line_ok = check_parsed($date, $time, $int_id, $flag, $address, $str, $id, $line);
        if ($is_line_ok) {
            my $created = "$date $time";
            if ($flag && $flag eq '<=' && $id) {
                $sth_message->execute($created, $int_id, $str, $id);
                next;
            }
            $sth_log->execute($created, $int_id, $str, $address);
        }
        else {
            print STDERR "ERROR: Cannot parse '$line'. PARAMS: \n" . Dumper($date, $time, $int_id, $flag, $address, $str) . "\n";
        }
    }
    else {
        print STDERR "ERROR: Cannot get int_id from line: '$line'. Required. Skip this line \n";
    }
}

$sth_message->finish();
$sth_log->finish();
close($fh);
$dbh->commit or die $DBI::errstr;
$dbh->disconnect;


##########################################################################################

sub check_parsed {
    my ($date, $time, $int_id, $flag, $address, $str, $id, $line) = @_;
    my $result = 1;
    if (!$date || !$time || !$int_id || !$str) {
        $result = 0;
    }
    if ($address && $address !~ m!^\w+\@[\w\-\.]+\.\w+$!) {
        $result = 0;
    }
    if ($int_id && $int_id !~ m!^\w{6}-\w{6}-\w{2}$!) {
        $result = 0;
    }
    if ($flag && $flag !~ m!^(?:<=|=>|->|\*\*|==)$!) {
        $result = 0;
    }
    if ($id && $id !~ m!^(?:id=.*)$!) {
        $result = 0;
    }
    return $result;
}

sub check_params {
    my ($file, $user_name, $password, $db_name, $db_port) = @_;
    my $is_ok = 0;
    if ($file && -s $file) {
        $is_ok = 1;
    }
    if ($user_name && defined($password) && $db_name && $db_port) {
        $is_ok = 1;
    }
    return $is_ok;
}

sub print_usage {
    my $usage = <<USAGE;
Usage: $0 --file <input_filename> --user_name <db username> --password <db password> --db_name <db name> --db_host <host> --db_port <port>
maillog file parser script

USAGE
    print $usage;
    exit 1;
}

1;