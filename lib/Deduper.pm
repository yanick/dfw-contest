package Deduper;

use 5.10.0;

use strict;
use warnings;

use Path::Iterator::Rule;
use MooseX::App::Simple;

parameter root_dir => (
    is => 'ro',
    required => 1,
    documentation => 'path to dedupe',
);

has file_iterator => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return Path::Iterator::Rule->new->iter_fast( $self->root_dir, {
            follow_symlinks => 0,
            sorted => 1,
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

# to take care of the cases where a, b, c and d are the same,
# and a,b and c, d  are different sets of hard-links.
has reported_inodes => (
    is => 'ro',
    default => sub { {} },
);

sub add_file {
    my( $self, $file ) = @_;

    push @{ $self->files->{$file->size} }, $file;
    if( my $nbr = $file->copies ) {
        $self->reported_inodes->{$file->inode} = $nbr;
    }
}

sub find_orig {
    my( $self, $file ) = @_;

    # do we have any file of the same size?
    my $candidates = $self->files->{$file->size}
        or return;

    for my $c ( @$candidates ) {
        return $c if $c->is_dupe( $file );
    }

    return;
}

sub is_dupe {
    my( $self, $file ) = @_;

    if ( my $orig = $self->find_orig( $file ) ) {
        return $orig;
    }

    $self->add_file($file);

    return;
}

sub seen_inodes {
    my( $self, $inode ) = @_;

    return unless keys %{ $self->reported_inodes };

    return unless $self->reported_inodes->{$inode}--;

    # no more copies to check? remove totally
    # should not be a big deal, but it's less
    # memory consumed
    delete $self->reported_inodes->{$inode} 
        unless $self->reported_inodes->{$inode};

    return 1;
}

sub next_dupe {
    my $self = shift;

    return if $self->finished;
    
    while( my $file = $self->file_iterator->() ) {
        next unless -f $file;
        $file = Deduper::File->new( path => $file );
        my $orig = $self->is_dupe($file) or next;
        next if $self->seen_inodes($file->inode);
        return $orig->path => $file->path;
    }

    $self->finished(1);
    return;
}

sub print_dupes {
    my( $self, $separator ) = @_;
    $separator ||= "\t";

    while( my @x = $self->next_dupe ) {
        say join $separator, @x;
    }
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

sub run {
    my $self = shift;

    $self->print_dupes;
}

__PACKAGE__->meta->make_immutable;

package Deduper::File;

use Moose;

has "path" => (
    is => 'ro',
    required => 1,
);

has "inode" => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return( (stat $self->path)[1] );
    },
);

has copies => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return( (stat $self->path)[3]-1 );
    },
);

has "size" => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return -s $self->path;
    },
);

# the "hash" is simply a 1024 bit segment in the middle
# of the file. Hopefully the middle part will deal with 
# similar headers and footers
has hash => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;

        # if the file is small, don't bother
        return '' if $self->size < 3 * 1024;

        open my $fh, '<', $self->path;
        my $hash;
        read $fh, $hash, 1024;

        return $hash;
    },
);

sub same_content {
    my( $self, $other ) = @_;

    open my $this, '<', $self->path;
    open my $that, '<', $other->path;

    # we know they are are same size
    my( $x, $y );
    while ( my $chunk = read $this, $x, 1024 ) {
        read $that, $y, 1024;
        return unless $x eq $y;
    }

    return 1;
}

sub is_dupe {
    my( $self, $other ) = @_;

    # if we are here, it's assumed the sizes are the same
    
    # special case: empty files are all the same
    return 1 unless $self->size;
    
    # different hashes?
    return unless $self->hash eq $other->hash;

    # go full metal diff on them
    return $self->same_content( $other );
}

__PACKAGE__->meta->make_immutable;

1;


