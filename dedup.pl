#!/usr/bin/perl 


use strict;
use warnings;

use Deduper;

my $dedup = Deduper->new( root_dir => shift );

$dedup->print_dupes;



