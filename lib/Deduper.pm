package Deduper;

use 5.10.0;

use strict;
use warnings;

use Path::Iterator::Rule;
use Moo;

has root_dir => (
    is => 'ro',
    required => 1,
);

has file_iterator => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return Path::Iterator::Rule->new->iter_fast( $self->root_dir, {
            follow_symlinks => 1,
        });
    },
);

has "finished" => (
    is => 'rw',
    default => 0,
);

has "files" => (
    is => 'ro',
    default => sub { {} },
);

sub is_dupe {
    my( $self, $file ) = @_;

    my $size = -s $file;

    if( my $orig = $self->files->{$size}[0] ) {
        return $orig->[0];
    }

    push @{ $self->files->{$size} }, [ $file ];

    return;
}

sub next_dupe {
    my $self = shift;

    return if $self->finished;
    
    while( my $file = $self->file_iterator->() ) {
        next unless -f $file;
        my $orig = $self->is_dupe($file) or next;
        return $orig => $file;
    }

    $self->finished(1);
    return;
}

sub all_dupes {
    my $self = shift;

    my %dupes;
    while ( my ( $orig, $dupe ) = $self->next_dupe ) {
        push @{ $dupes{$orig} }, $dupe;
    }

    # we want them all nice and sorted
    my @dupes;
    while( my( $orig, $dupes ) = each %dupes ) {
        push @dupes, [ sort $orig, @$dupes ];
    }

    return sort { $a->[0] cmp $b->[0] } @dupes;
}

1;


