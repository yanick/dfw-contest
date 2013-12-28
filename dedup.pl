#!/usr/bin/perl 

use 5.10.0;

use strict;
use warnings;

use Path::Iterator::Rule;


sub deduple {
    my $iter = Path::Iterator::Rule->new->iter_fast( shift, {
        follow_symlinks => 1,
    });

    while( my $file = $iter->() ) {
    }
}



