package Deduper;

use 5.10.0;

use strict;
use warnings;

use Path::Iterator::Rule;
use List::MoreUtils qw/ uniq /;

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
        return Path::Iterator::Rule->new->file->iter_fast( $self->root_dir, {
            follow_symlinks => 0,
            sorted => 0,
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

sub all_files {
    my $self = shift;
    
    my @files;
    for my $v ( values %{ $self->files } ) {
        if ( ref $v eq 'ARRAY' ) {
            push @files, @$v;
        }
        else {
            push @files, map { @$_ } values %$v;
        }
    }

    return @files;
}

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

    if( $self->stats ) {
        $self->meta->add_after_method_modifier( run => \&print_stats );
    }

    $self->meta->make_immutable;
}

sub add_file {
    my( $self, $file ) = @_;

    if( my $ref = $self->files->{$file->size} ) {
        if ( ref $ref  eq 'ARRAY' ) {
            $self->files->{$file->size} = {};
            for( @$ref, $file ) {
                push @{$self->files->{$file->size}{$file->hash}}, $file;
            }
        }
        else {
            push @{$ref->{$file->hash}}, $file;
        }
    }
    else {
        # nothing yet, just push the sucker in
        $self->files->{$file->size} = [ $file ];
    }


    if( my $nbr = $file->copies ) {
        $self->reported_inodes->{$file->inode} = $nbr;
    }
}

sub find_orig {
    my( $self, $file ) = @_;

    # do we have any file of the same size?
    my $candidates = $self->files->{$file->size}
        or return;

    if( ref $candidates eq 'HASH' ) {
        $candidates = $candidates->{$file->hash};
    }
    elsif ( $candidates->[0]->hash ne $file->hash ) {
        return;
    }

    # first check if any share the same inode
    my $inode = $file->inode;
    for ( @$candidates ) {
        return $_ if $_->inode == $inode;
    }

    # then check if dupes
    for ( @$candidates ) {
        return $_ if $_->is_dupe($file);
    }

    return;
}

sub is_dupe {
    my( $self, $file ) = @_;

    return $_ for $self->find_orig($file);

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
        return Deduper::File->new( path => $file );
    }

    $self->finished(1);
    return;
}

sub next_dupe {
    my $self = shift;

    return if $self->finished;

    while( my $file = $self->file_iterator->() ) {
        $file = Deduper::File->new( path => $file );
        my $orig = $self->is_dupe($file) or next;
        # next if $self->seen_inodes($file->inode);
        return $orig => $file;
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
        push @{ $dupes{$orig->path} }, $orig, $dupe;
    }

    # we want them all nice and sorted
    my @dupes;
    while( my( $orig, $dupes ) = each %dupes ) {
        my %seen_inode;
        push @dupes, [ grep { not $seen_inode{ $_->inode }++ } uniq sort {
            $a->path cmp $b->path }  @$dupes ];
    }

    # filter out the dupes that are just hard links
    @dupes = grep { @$_ > 1 } @dupes;

    return sort { $a->[0]->path cmp $b->[0]->path } @dupes;
}

sub run {
    my $self = shift;

    # $self->print_dupes;
    for my $entry ( $self->all_dupes ) {
        say join "\t", map { $_->path } @$entry;
    }
}

sub print_stats {
    my $self = shift;

    say '-' x 30;
    say "time taken: ", time - $self->start_time, " seconds";

    my $nbr_files;
    my $nbr_hash;
    my $nbr_end_hash;
    my $nbr_md5;

    for my $f ( $self->all_files ) {
        $nbr_files++;
        $nbr_hash++ if $f->has_hash;
        $nbr_end_hash++ if $f->has_end_hash;
        $nbr_md5++ if $f->has_md5;
    }

    say join " ", $nbr_files, $nbr_hash, $nbr_end_hash, $nbr_md5;
}

__PACKAGE__->meta->make_immutable;

package Deduper::File;

use Moose;
use MooseX::ClassAttribute;
use Digest::xxHash;

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
    predicate => 'has_inode',
    default => sub {
        my $self = shift;
        return( (stat $self->path)[1] );
    },
);

has copies => (
    is => 'ro',
    lazy => 1,
    predicate => 'has_copies',
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

        sysopen my $fh, $self->path, 0;
        my $digest = Digest::xxHash->new(613);
        my $chunk;
        while( sysread $fh, $chunk, 1024 * 1024 ) {
            $digest->add($chunk);
        }
        return $digest->digest;
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

        sysopen my $fh, $self->path, 0;
        sysread $fh, my $hash, $self->hash_size;
        return $hash;
    },
);

has end_hash => (
    is => 'ro',
    lazy => 1,
    predicate => 'has_end_hash',
    default => sub {
        my $self = shift;

        sysopen my $fh, $self->path, 0;
        sysseek $fh, -$self->hash_size, 2;
        sysread $fh, my $hash, $self->hash_size;
        return $hash;
    },
);

sub is_dupe {
    my( $self, $other ) = @_;

    # if we are here, it's assumed the sizes are the same
    # and the beginning hashes are the same
    
    # different hashes?
    return 
           $self->end_hash eq $other->end_hash
        && $self->digest eq $other->digest;
}

__PACKAGE__->meta->make_immutable;

1;


