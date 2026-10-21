#!/usr/bin/env perl
# Normalizes "artemis check cluster" output for cluster.sh: each node ID
# becomes the port of the primary the topology gives it, the primaries'
# local times, the time variance and log times are masked, and the rows of
# a table and the "--> Verifying" blocks, in the order of a topology the
# brokers keep in no fixed order, are sorted.
use strict;
use warnings;

my @lines = map { s/\r?\n\z//r } <STDIN>;
my %port;
for (@lines) {
  $port{$1} = $2 if /^\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\s+\|\s+tcp:\/\/[^:]+:(\d+)/;
}
sub node { "node\@" . ($port{ $_[0] } // "?") }
for (@lines) {
  # In a table, padded to the ID's width.
  s/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})(?=\s+\|)/sprintf "%-36s", node($1)/ge;
  s/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/node($1)/ge;
  s/^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} /<log time> /;
  s/\d{4}-\d\d-\d\d \d\d:\d\d:\d\d/<time>             /g;
  s/(Time variance in the cluster is )\d+( milliseconds)/$1<n>$2/;
}
my (@out, @rows, @blocks);
my $flush_rows = sub { push @out, sort @rows; @rows = () };
for (@lines) {
  if (/^--> /) {
    $flush_rows->();
    push @blocks, [$_];
  } elsif (@blocks && !/^\*+$/) {
    push @{ $blocks[-1] }, $_;
  } elsif (/^\s*node@\S+\s+\|/) {
    push @rows, $_;
  } else {
    $flush_rows->();
    push @out, $_;
  }
}
$flush_rows->();
push @out, map { @$_ } sort { $a->[0] cmp $b->[0] } @blocks;
print "$_\n" for @out;
