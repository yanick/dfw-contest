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

option small_file => (
    isa => 'Int',
    is => 'ro',
    trigger => sub {
        my $self = shift;
        Deduper::File->small_file( $self->small_file );
    },
    documentation => 'minimal size for files to be hashed',
);

option chunk => (
    isa => 'Int',
    is => 'ro',
    trigger => sub {
        my $self = shift;
        Deduper::File->chunk_size( $self->chunk );
    },
    documentation => 'size of the chunks to read when comparing',
);

option hash_size => (
    isa => 'Int',
    is => 'ro',
    trigger => sub {
        my $self = shift;
        Deduper::File->hash_size( $self->hash_size );
    },
    documentation => 'size of the file hash',
);

option stats => (
    isa => 'Bool',
    is => 'ro',
    documentation => 'report statistics',
);

option max_files => (
    traits => [ 'Counter' ],
    isa => 'Int',
    is => 'ro',
    predicate => 'has_max_files',
    default => 0,
    documentation => 'max number of files to scan (for testing)',
    handles => {
        dec_files_to_scan => 'dec',
    },
);

has start_time => (
    is => 'ro',
    isa => 'Int',
    default => sub { 0 + time },
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

sub BUILD {
    my $self = shift;

    $self->meta->make_mutable;
    
    if( $self->max_files > 0 ) {
        $self->meta->add_after_method_modifier(
            next_file => sub {
                my $self = shift;
                $self->dec_files_to_scan;
                $self->finished(1) unless $self->max_files > 0;
            }
        );
    }

    $self->meta->make_immutable;
}

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

    # first check if any share the same inode
    for my $c ( @$candidates ) {
        return $c if $c->inode == $file->inode;
    }

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

sub next_file {
    my $self = shift;

    return if $self->finished;
    
    while( my $file = $self->file_iterator->() ) {
        next unless -f $file;

        return Deduper::File->new( path => $file );
    }

    $self->finished(1);
    return;
}

sub next_dupe {
    my $self = shift;

    while( my $file = $self->next_file ) {
        my $orig = $self->is_dupe($file) or next;
        next if $self->seen_inodes($file->inode);
        return $orig->path => $file->path;
    }

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

    if( $self->stats ) {
        warn "time taken: ", time - $self->start_time, " seconds\n";

        my $nbr_files;
        my $nbr_hash;
        my $nbr_md5;

        for my $f ( values %{ $self->files } ) {
            $nbr_files += @$f;
            for my $j ( @$f ) {
                $nbr_hash++ if $j->has_hash;
                $nbr_md5++ if $j->has_md5;
            }
        }

        warn join " ", $nbr_files, $nbr_hash, $nbr_md5, "\n";

    }

}

__PACKAGE__->meta->make_immutable;

package Deduper::File;

use Moose;
use MooseX::ClassAttribute;
use Digest::MD5;

class_has small_file => (
    isa => 'Int',
    is => 'rw',
    default => 1024,
);

class_has chunk_size => (
    isa => 'Int',
    is => 'rw',
    default => 1024,
);

class_has hash_size => (
    isa => 'Int',
    is => 'rw',
    default => 1024,
);

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

has digest => (
    is => 'ro',
    lazy => 1,
    predicate => 'has_md5',
    default => sub {
        my $self = shift;

        open my $fh, '<', $self->path;
        my $ctx = Digest::MD5->new;
        $ctx->addfile($fh);
        return $ctx->digest;
    },
);

# the "hash" is simply a 1024 bit segment in the middle
# of the file. Hopefully the middle part will deal with 
# similar headers and footers
has hash => (
    is => 'ro',
    lazy => 1,
    predicate => 'has_hash',
    default => sub {
        my $self = shift;

        my $size = $self->size;

        # if the file is small, don't bother
        return '' if $self->size <= $self->small_file;

        open my $fh, '<', $self->path;
        read $fh, my $hash, $self->hash_size;
        return $hash;
    },
);

sub same_content {
    my( $self, $other ) = @_;

    open my $this, '<', $self->path;
    open my $that, '<', $other->path;

    my $size = $self->chunk_size;

    # we know they are are same size
    my( $x, $y );
    while ( read $this, $x, $size ) {
        read $that, $y, $size;
        return unless $x eq $y;
    }

    return 1;
}

sub is_dupe {
    my( $self, $other ) = @_;

    # if we are here, it's assumed the sizes are the same
    
    # different hashes?
    return unless $self->hash eq $other->hash;

    # go full metal diff on them
    return $self->digest eq $other->digest;
}

__PACKAGE__->meta->make_immutable;

1;


