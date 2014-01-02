requires 'Path::Iterator::Rule';
requires 'Moose';
requires 'MooseX::App::Simple';
requires 'MooseX::ClassAttribute';
requires 'Digest::xxHash';
requires 'List::MoreUtils';

on test => sub {
	requires 'Test::Deep';
};
