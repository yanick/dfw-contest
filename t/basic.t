use strict;
use warnings;

use Test::More;
use Test::Deep qw/ deep_diag cmp_deeply /;

use Deduper;

my $output;
open STDOUT, '>', \$output;

my $deduper = Deduper->new( root_dir => 'corpus' );

my @all_dupes = $deduper->all_dupes;

my @expected = (
   [
     'corpus/1/a',
     'corpus/1/b',
   ],
);

for my $exp ( @expected ) {
    my $d = shift @all_dupes;
    next if cmp_deeply $d => $exp;
    diag explain [ $d, $exp ];
}

ok !@all_dupes, "no leftovers"
    or diag explain \@all_dupes;

done_testing;

