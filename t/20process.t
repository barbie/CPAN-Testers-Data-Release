#!/usr/bin/perl -w
use strict;

# testing proces with idfile

use CPAN::Testers::Data::Release;
use Test::More tests => 9;

my $config = 't/_DBDIR/10attributes.ini';
my $idfile = 't/_DBDIR/idfile.txt';

my $obj;
eval { $obj = CPAN::Testers::Data::Release->new(config => $config) };
isa_ok($obj,'CPAN::Testers::Data::Release');

SKIP: {
    skip "Problem creating object", 2 unless($obj);

    is(lastid(),0,'.. lastid is 0 at start');

    my @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 11, "row count for release_summary");

    $obj->{clean} = 1;
    $obj->process;

    is(lastid(),0,'.. lastid is 0 after clean');

    @rows = $obj->{CPANSTATS}{dbh}->get_query('hash','select count(*) as count from release_summary');
    is($rows[0]->{count}, 10, "row count for release_summary");

    $obj->{clean} = 0;
    $obj->process;  # from start
    
    is(lastid(),9348321,'.. lastid is 0 after from start');

    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");

    $obj->process;  # from last

    @rows = $obj->{RELEASE}{dbh}->get_query('hash','select count(*) as count from release');
    is($rows[0]->{count}, 9, "row count for release");

    is(lastid(),9348321,'.. lastid is 0 after from last');
}

sub lastid {
    my $lastid = 0;

    if(-f $idfile) {
        if(my $fh = IO::File->new($idfile,'r')) {
            my @lines = <$fh>;
            ($lastid) = $lines[0] =~ /(\d+)/;
            $fh->close;
        }
    }

    return $lastid;
}