#!/usr/bin/perl -w
use strict;

use CPAN::Testers::Data::Release;
use File::Spec;
use Test::More tests => 5;

my $config = File::Spec->catfile('t','_DBDIR','test-config.ini');
my $idfile = File::Spec->catfile('t','_DBDIR','idfile.txt');

my $obj;
eval { $obj = CPAN::Testers::Data::Release->new(config => $config) };
isa_ok($obj,'CPAN::Testers::Data::Release');

SKIP: {
    skip "Problem creating object", 2 unless($obj);

    my @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 11, "row count for release_summary");

    $obj->{clean} = 1;
    $obj->process;

    @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 10, "row count for release_summary");

    $obj->{clean} = 0;
    $obj->process;  # from start
    
    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");

    $obj->process;  # from last

    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");
}
