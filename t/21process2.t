#!/usr/bin/perl -w
use strict;

# testing process with no id file

use CPAN::Testers::Data::Release;
use Test::More tests => 9;

my $config = 't/_DBDIR/test-config.ini';
my $idfile = 't/_DBDIR/idfile.txt';
unlink $idfile if -f $idfile;

my $obj;
eval { $obj = CPAN::Testers::Data::Release->new(config => $config) };
isa_ok($obj,'CPAN::Testers::Data::Release');

SKIP: {
    skip "Problem creating object", 2 unless($obj);

    is(-f $idfile,undef,'.. no idfile at start');

    my @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 10, "row count for release_summary");

    $obj->{clean} = 1;
    $obj->process;

    is(-f $idfile,undef,'.. no idfile after clean');

    @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 10, "row count for release_summary");

    $obj->{clean} = 0;
    $obj->process;  # from start
    
    is(-f $idfile,undef,'.. no idfile after from start');

    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");

    $obj->process;  # from last

    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");

    is(-f $idfile,undef,'.. no idfile after from last');
}
