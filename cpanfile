requires 'Path::Iterator::Rule';
requires 'Moose';
requires 'MooseX::App::Simple';

on test => sub {
	requires 'Test::Deep';
};
