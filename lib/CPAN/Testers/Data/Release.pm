package CPAN::Testers::Data::Release;

use strict;
use warnings;

use vars qw($VERSION);
$VERSION = '0.03';

#----------------------------------------------------------------------------
# Library Modules

use base qw(Class::Accessor::Fast);

use CPAN::Testers::Common::DBUtils;
use Config::IniFiles;
use File::Basename;
use File::Path;
use Getopt::Long;
use IO::File;

#----------------------------------------------------------------------------
# Variables

my (%backups);

my %phrasebook = (
    # MySQL database
    'SelectAll'         => 'SELECT dist,version,pass,fail,na,unknown FROM release_summary WHERE perlmat=1 ORDER BY dist',
    'SelectRows'        => 'SELECT * FROM release_summary ORDER BY dist',
    'DelRows'           => 'DELETE FROM release_summary WHERE dist=?',
    'AddRow'            => 'INSERT INTO release_summary (dist,version,id,guid,oncpan,distmat,perlmat,patched,pass,fail,na,unknown) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',

    'SelectDists'       => 'SELECT dist,version FROM release_summary WHERE id > ?',
    'DelRow'            => 'DELETE FROM release_summary WHERE dist=? AND version=?',

    # SQLite database
    'CreateTable'       => 'CREATE TABLE release (dist text not null, version text not null, pass integer not null, fail integer not null, na integer not null, unknown integer not null)',
    'CreateDistIndex'   => 'CREATE INDEX release__dist ON release ( dist )',
    'CreateVersIndex'   => 'CREATE INDEX release__version ON release ( version )',

    'DeleteAll'         => 'DELETE FROM release',
    'InsertRelease'     => 'INSERT INTO release (dist,version,pass,fail,na,unknown) VALUES (?,?,?,?,?,?)',
    'UpdateRelease'     => 'UPDATE release SET pass=?,fail=?,na=?,unknown=? WHERE dist=? AND version=?',
    'SelectRelease'     => 'SELECT * FROM release WHERE dist=? AND version=?',
);
#----------------------------------------------------------------------------
# The Application Programming Interface

sub new {
    my $class = shift;

    my $self = {};
    bless $self, $class;

    $self->_init_options(@_);
    return $self;
}

sub DESTROY {
    my $self = shift;
}

__PACKAGE__->mk_accessors(qw( dbx logfile logclean ));

sub process {
    my $self = shift;
    if($self->{clean}) { $self->clean() }
    else               { $self->backup() }
}

sub backup {
    my $self = shift;
    my $db = $self->dbx;

    $self->_log("Create backup databases");

    # write to a clean database
    for my $driver (keys %backups) {
        if($backups{$driver}{'exists'}) {
            $backups{$driver}{db}->do_query($phrasebook{'DeleteAll'});
        } elsif($driver =~ /SQLite/i) {
            $backups{$driver}{db}->do_query($phrasebook{'CreateTable'});
            $backups{$driver}{db}->do_query($phrasebook{'CreateDistIndex'});
            $backups{$driver}{db}->do_query($phrasebook{'CreateVersIndex'});
        } else {
            $backups{$driver}{db}->do_query($phrasebook{'CreateTable'});
        }
    }

    $self->_log("Backup via DBD drivers");

    # store data from master database
    my %data;
    my $dist = '';
    my $rows = $db->iterator('array',$phrasebook{'SelectAll'});
    while(my $row = $rows->()) {
        if($dist && $dist ne $row->[0]) {
            $self->_log("... dist=$dist");

            # write data out
            for my $driver (keys %backups) {
                for my $vers (keys %data) {
                    $backups{$driver}{db}->do_query($phrasebook{'InsertRelease'},@{ $data{$vers} });
                }
            }

	        %data = ();
	    }

        $dist = $row->[0];

        if($data{$row->[0]} && $data{$row->[0]}{$row->[1]}) {
            $data{$row->[0]}{$row->[1]}->[2] += $row->[2];
            $data{$row->[0]}{$row->[1]}->[3] += $row->[3];
            $data{$row->[0]}{$row->[1]}->[4] += $row->[4];
            $data{$row->[0]}{$row->[1]}->[5] += $row->[5];
        } else {
            $data{$row->[1]} = $row;
        }
    }

    if($dist) {
        $self->_log("... dist=$dist");
        # write data out
        for my $driver (keys %backups) {
            for my $vers (keys %data) {
                $backups{$driver}{db}->do_query($phrasebook{'InsertRelease'},@{ $data{$vers} });
            }
        }
    }

    # handle the CSV exception
    if($backups{CSV}) {
        $self->_log("Backup to CSV file");
        $backups{CSV}{db} = undef;  # close db handle
        my $fh1 = IO::File->new('release','r') or die "Cannot read temporary database file 'release'\n";
        my $fh2 = IO::File->new($backups{CSV}{dbfile},'w+') or die "Cannot write to CSV database file $backups{CSV}{dbfile}\n";
        while(<$fh1>) { print $fh2 $_ }
        $fh1->close;
        $fh2->close;
        unlink('release');
    }
}

# sub to remove duplicates in the matser database.
sub clean {
    my $self = shift;
    my $db = $self->dbx;

    $self->_log("Clean master database");

    my %data;
    my $dist = '';
    my $rows = $db->iterator('hash',$phrasebook{'SelectRows'});
    while(my $row = $rows->()) {
        if($dist && $dist ne $row->{dist}) {
    	    $db->do_query($phrasebook{'DelRows'},$dist);
            $self->_log("DelRows: $dist");
            for my $vers (keys %data) {
                for my $code (keys %{$data{$vers}}) {
                    my $rowx = $data{$vers}{$code};
                        $db->do_query($phrasebook{'AddRow'},$dist,$vers,
                    $rowx->{id},$rowx->{guid},
                    $rowx->{oncpan},$rowx->{distmat},$rowx->{perlmat},$rowx->{patched},
                    $rowx->{pass},$rowx->{fail},$rowx->{na},$rowx->{unknown});
                            $self->_log('AddRow: ' . join(', ',
                    $dist,$vers,
                    $rowx->{id},$rowx->{guid},
                    $rowx->{oncpan},$rowx->{distmat},$rowx->{perlmat},$rowx->{patched},
                    $rowx->{pass},$rowx->{fail},$rowx->{na},$rowx->{unknown}) );
                }
            }

            %data = ();
        }

        $dist = $row->{dist};
        my $code = join(':',$row->{oncpan},$row->{distmat},$row->{perlmat},$row->{patched});
        $data{$row->{version}}{$code} = $row;
    }

    if($dist) {
        $db->do_query($phrasebook{'DelRows'},$dist);
        $self->_log("DelRows: $dist");
        for my $vers (keys %data) {
	    for my $code (keys %{$data{$vers}}) {
	        my $rowx = $data{$vers}{$code};
                $db->do_query($phrasebook{'AddRow'},$dist,$vers,
		    $rowx->{id},$rowx->{guid},
		    $rowx->{oncpan},$rowx->{distmat},$rowx->{perlmat},$rowx->{patched},
		    $rowx->{pass},$rowx->{fail},$rowx->{na},$rowx->{unknown});
                $self->_log('AddRow: ' . join(', ',
		    $dist,$vers,
		    $rowx->{id},$rowx->{guid},
		    $rowx->{oncpan},$rowx->{distmat},$rowx->{perlmat},$rowx->{patched},
		    $rowx->{pass},$rowx->{fail},$rowx->{na},$rowx->{unknown}) );
	    }
	}
    }
}

sub help {
    my ($self,$full,$mess) = @_;

    print "\n$mess\n\n" if($mess);

    if($full) {
        print <<HERE;

Usage: $0 --config=<file> [-h] [-v]

  --config=<file>   database configuration file
  --clean           clean master database of duplicates
  -h                this help screen
  -v                program version

HERE

    }

    print "$0 v$VERSION\n\n";
    exit(0);
}


#----------------------------------------------------------------------------
# Internal Methods

sub _init_options {
    my $self = shift;
    my %hash  = @_;
    my %options;

    GetOptions( \%options,
        'clean',
        'config=s',
        'help|h',
        'version|v'
    ) or help(1);

    # default to API settings if no command line option
    for(qw(config help version)) {
        $options{$_} ||= $hash{$_}  if(defined $hash{$_});
    }

    $self->help(1)  if($options{help});
    $self->help(0)  if($options{version});

    $self->help(1,"Must specific the configuration file")               unless($options{config});
    $self->help(1,"Configuration file [$options{config}] not found")    unless(-f $options{config});

    # load configuration
    my $cfg = Config::IniFiles->new( -file => $options{config} );

    $self->logfile(  $cfg->val('MASTER','logfile'  ) );
    $self->logclean( $cfg->val('MASTER','logclean' ) || 0 );

    # configure upload DB
    $self->help(1,"No configuration for CPANSTATS database") unless($cfg->SectionExists('CPANSTATS'));
    my %opts = map {$_ => ($cfg->val('CPANSTATS',$_) || undef);} qw(driver database dbfile dbhost dbport dbuser dbpass);
    my $db = CPAN::Testers::Common::DBUtils->new(%opts);
    $self->help(1,"Cannot configure CPANSTATS database") unless($db);
    $self->dbx($db);

    $self->help(1,"No configuration for BACKUPS with backup option")    unless($cfg->SectionExists('BACKUPS'));
    my @drivers = $cfg->val('BACKUPS','drivers');
    for my $driver (@drivers) {
        $self->help(1,"No configuration for backup option '$driver'")   unless($cfg->SectionExists($driver));

        my %opt = map {$_ => ($cfg->val($driver,$_)||undef)} qw(driver database dbfile dbhost dbport dbuser dbpass);
        $backups{$driver}{'exists'} = $driver =~ /SQLite/i ? -f $opt{database} : 1;

        # CSV is a bit of an oddity!
        if($driver =~ /CSV/i) {
            $backups{$driver}{'exists'} = 0;
            $backups{$driver}{'dbfile'} = $opt{dbfile};
            $opt{dbfile} = 'release';
            unlink($opt{dbfile});
        }

        $backups{$driver}{db} = CPAN::Testers::Common::DBUtils->new(%opt);
        $self->help(1,"Cannot configure BACKUPS database for '$driver'")   unless($backups{$driver}{db});
    }

    $self->{clean} = 1 if($options{clean});
}

sub _log {
    my $self = shift;
    my $log = $self->logfile or return;
    mkpath(dirname($log))   unless(-f $log);

    my $mode = $self->logclean ? 'w+' : 'a+';
    $self->logclean(0);

    my @dt = localtime(time);
    my $dt = sprintf "%04d/%02d/%02d %02d:%02d:%02d", $dt[5]+1900,$dt[4]+1,$dt[3],$dt[2],$dt[1],$dt[0];

    my $fh = IO::File->new($log,$mode) or die "Cannot write to log file [$log]: $!\n";
    print $fh "$dt ", @_, "\n";
    $fh->close;
}

q{Written to the tune of Release by Pearl Jam :)};

__END__

=head1 NAME

CPAN::Testers::Data::Release - CPAN Testers Release database generator

=head1 SYNOPSIS

  perl release.pl --config=<file>

=head1 DESCRIPTION

This distribution contains the code that extracts the data from the 
release_summary table in the cpanstats database. The data extracted represents 
the data relating to the public releases of Perl, i.e. no patches and official 
releases only.

=head1 SQLite DATABASE

The database created uses the following schema:

  CREATE TABLE release (
      dist    text    not null,
      version text    not null,
      pass    integer not null,
      fail    integer not null,
      na      integer not null,
      unknown integer not null
  );

  CREATE INDEX release__dist ON release ( dist );
  CREATE INDEX release__version ON release ( version );

=head1 INTERFACE

=head2 The Constructor

=over

=item * new

Instatiates the object CPAN::Testers::Data::Release:

  my $obj = CPAN::Testers::Data::Release->new();

=back

=head2 Public Methods

=over

=item * process

Shorthand function to run methods based on command line options.

=item * backup

Run backup processes.

=item * clean

Run database table clean processes.

=item * help

Provides basic help screen.

=back

=head2 Private Methods

=over

=item * _init_options

Extracts the command line options and performs basic validation.

=back

=head1 BECOME A TESTER

Whether you have a common platform or a very unusual one, you can help by
testing modules you install and submitting reports. There are plenty of
module authors who could use test reports and helpful feedback on their
modules and distributions.

If you'd like to get involved, please take a look at the CPAN Testers Wiki,
where you can learn how to install and configure one of the recommended
smoke tools.

For further help and advice, please subscribe to the the CPAN Testers
discussion mailing list.

  CPAN Testers Wiki - http://wiki.cpantesters.org
  CPAN Testers Discuss mailing list
    - http://lists.cpan.org/showlist.cgi?name=cpan-testers-discuss

=head1 BUGS, PATCHES & FIXES

There are no known bugs at the time of this release. However, if you spot a
bug or are experiencing difficulties, that is not explained within the POD
documentation, please send bug reports and patches to the RT Queue (see below).

Fixes are dependant upon their severity and my availablity. Should a fix not
be forthcoming, please feel free to (politely) remind me.

RT: http://rt.cpan.org/Public/Dist/Display.html?Name=CPAN-Testers-Data-Release

=head1 SEE ALSO

L<CPAN::Testers::Data::Generator>
L<CPAN::Testers::Data::Uploads>

F<http://www.cpantesters.org/>,
F<http://stats.cpantesters.org/>,
F<http://wiki.cpantesters.org/>,
F<http://blog.cpantesters.org/>

=head1 AUTHOR

  Barbie <barbie@cpan.org> 2009-present

=head1 COPYRIGHT AND LICENSE

  Copyright (C) 2009-2011 Barbie <barbie@cpan.org>

  This module is free software; you can redistribute it and/or
  modify it under the same terms as Perl itself.

=cut
