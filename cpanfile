requires 'Path::Iterator::Rule';
requires 'Moose';
requires 'MooseX::App::Simple';
requires 'MooseX::ClassAttribute';

on test => sub {
	requires 'Test::Deep';
};
