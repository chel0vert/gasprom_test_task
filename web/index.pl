#!/usr/bin/env perl

use strict;
use warnings;

use Data::Dumper;

use CGI;
use DBI;

my $cgi = new CGI;
print $cgi->header, "\n";

my $q = CGI::Vars();
my $address = $q->{'address'};
my ($user_name, $password, $db_name, $db_host, $db_port) = ('test_user', '', 'test_db', 'localhost', '5432');

my $datasource = "dbi:Pg:dbname=$db_name;host=$db_host;port=$db_port";
my $dbh = DBI->connect_cached($datasource, $user_name, $password, { AutoCommit => 0, RaiseError => 1 }) or die $DBI::errstr;
my $query = <<QUERY;
(
    (SELECT message.created, message.str, message.int_id FROM message WHERE message.int_id IN
        (SELECT distinct(log.int_id) FROM log where log.address = ?))
        UNION
    (SELECT log.created, log.str, log.int_id FROM log WHERE log.address = ?)
)
ORDER BY int_id, created
QUERY
my $sth = $dbh->prepare($query);
$sth->execute($address, $address);
print <<HTML;
<!DOCTYPE html>
<html>
<head>
    <title>Result</title>
    <link rel="stylesheet" href="style.css">
</head>
<body>
<div style="color:red"></div>
<table>
HTML

my $result = $sth->fetchall_arrayref;
my $count = scalar @{$result};
if ($count > 100) {
    print "<div style='color:red'>Total rows: $count</div>";
}

my $counter = 1;
foreach my $row (@{$result}[0 .. 99]) {
    my $timestamp = $row->[0];
    my $str = $row->[1];
    $str =~ s!<!&lt;!g;
    $str =~ s!>!&gt;!g;
    my $class = "row_" . ($counter % 2);
    my $item_template = <<TEMPLATE;
  <tr class="$class"><td>$counter</td><td>$timestamp</td><td>$str</td></tr>
TEMPLATE
    print $item_template;
    $counter++;
}

$sth->finish();
$dbh->commit or die $DBI::errstr;
$dbh->disconnect;

print <<FOOTER;
</table>
</body>
</html>
FOOTER


1;