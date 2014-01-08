#!/usr/bin/env perl

=head1 SYNOPSIS

    dedup.pl <root dir>


=head1 DESCRIPTION

This entry is for the 'Most Modern' category. Basically, my goal was to see
how succinct and clean I could go by leveraging the modern tools at my
disposal -- while maintaining a decent speed.

The result? 

=over

=item *

I'm using L<MooseX::App::Simple> to take care of all the command-line
stuff -- dealing with the parameter, the options, and the automatic generation
of the help (do C<dedup.pl --help> to see what I mean) 

=item * 

To traverse the directories, I'm using L<Path::Iterator::Rule>, which takes
care of symlinks for me, and reduce the whole exercise to simply calling an 
iterator.

=item *

For a token nod to efficiency, L<MooseX::XSAccessor> to make accessors a
little more speedy (although this is defeated by most of my attribute being
lazy), and L<MooseX::ClassAttribute>, which saves quite a lot of memory by not
storing the chunk size for every single file object.


=back

=head2 The Algorithm

Algorithm-wise, this solution doesn't offer anything ground-breaking. I keep a
hash of the seen files based on the file size and, if necessary, the first few
bytes of the files (by default, 1024 bytes). If the beginning of the files
match, I'll then compare the tail end of the files (to get around files that
have the same preamble) and, if that also match, finally resort to an xxHash.

Tests on the initial /dedup and on my own system show that the sorting by file
sizes already weed out a truckload of files. And for the ones remaining, the
beginning-of-file comparison is very efficient. The efficiency of the tail-end
snippet is not as proeminent, but since it ends up anyway being done on a very
small number of files and thus doesn't take a lot of cycles, I've kept it for
giggles.

=head2 Performance

From what I've seen, it seems that on '/dedup' the
performances are at least comparable to the baseline numbers. Which is
not too shabby (or so I think), considering that I'm using chubby modules like
Moose and MooseX::App, and objects everywhere 

For '/more-dedup', the script unfortunately bursts above 500M of memory, which I
deemed to be too much. Having more time, I could have turned the
C<Deduper::File> objects into something more lean. Oh well. :-)

=cut

use 5.10.0;

package Deduper;

use strict;
use warnings;

use Path::Iterator::Rule;
use List::MoreUtils qw/ uniq /;

use MooseX::App::Simple;

use MooseX::XSAccessor;

parameter root_dir => (
    is => 'ro',
    required => 1,
    documentation => 'path to dedupe',
);

option hash_size => (
    isa => 'Int',
    is => 'ro',
    default => '1024',
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

has finished => (
    is => 'rw',
    default => 0,
);

has files => (
    is => 'ro',
    default => sub { {} },
);

sub all_files {
    my $self = shift;

    my @files;
    for my $v ( values %{ $self->files } ) {
        push @files, ref $v eq 'Deduper::File' ? $v : map { @$_ } values %$v;
    }

    return @files;
}

sub BUILD {
    my $self = shift;

    $self->meta->make_mutable;

    $self->meta->add_after_method_modifier(
        next_file => sub {
            my $self = shift;
            $self->dec_files_to_scan;
            $self->finished(1) unless $self->max_files > 0;
        }
    ) if $self->max_files;

    $self->meta->add_after_method_modifier( run => \&print_stats )
        if $self->stats;

    $self->meta->make_immutable;
}

sub add_file {
    my( $self, $file ) = @_;

    if( my $ref = $self->files->{$file->size} ) {
        if ( ref $ref  eq 'Deduper::File' ) {
            $ref = $self->files->{$file->size} = { $ref->hash => [ $ref ] };
        }
        push @{$ref->{$file->hash}}, $file;
    }
    else {
        # nothing yet, just push the sucker in
        $self->files->{$file->size} = $file;
    }

}

sub find_orig {
    my( $self, $file ) = @_;

    # do we have any file of the same size?
    my $candidates = $self->files->{$file->size}
        or return;

    my @c;

    if( ref $candidates eq 'Deduper::File' ) {
        return if $candidates->hash ne $file->hash;
        @c = ( $candidates );
    }
    else {
        @c = @{ $candidates->{$file->hash} || return };
    }

    # first check if any share the same inode
    my $inode = $file->inode;
    for ( @c ) {
        return $_ if $_->inode == $inode;
    }

    # then check if dupes
    for ( @c ) {
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

    say join "\t", map { $_->path } @$_ for $self->all_dupes;
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
        $nbr_hash++     if $f->has_hash;
        $nbr_end_hash++ if $f->has_end_hash;
        $nbr_md5++      if $f->has_md5;
    }

    say join " / ", $nbr_files, $nbr_hash, $nbr_end_hash, $nbr_md5;
}

__PACKAGE__->meta->make_immutable;

package Deduper::File;

use Digest::xxHash;

use Moose;
use MooseX::ClassAttribute;
use MooseX::XSAccessor;

class_has hash_size => (
    isa => 'Int',
    is => 'rw',
    default => 1024,
);

has path => (
    is => 'ro',
    required => 1,
);

has inode => (
    is => 'ro',
    lazy => 1,
    predicate => 'has_inode',
    default => sub {
        my $self = shift;
        return( (stat $self->path)[1] );
    },
);

has size => (
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

package main;

# modulino time!

Deduper->new_with_options->run unless caller;
