use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME'              => 'CPAN::Testers::Data::Release',
    'VERSION_FROM'      => 'lib/CPAN/Testers/Data/Release.pm',
    'PREREQ_PM'         => {
        'CPAN::Testers::Common::DBUtils'    => 0,
        'Config::IniFiles'                  => 0,
        'File::Basename'                    => 0,
        'File::Path'                        => 0,
        'Getopt::Long'                      => 0,
        'IO::File'                          => 0,
    },
    'NO_META'           => 1,

    ($] >= 5.005 ?    ## Add these new keywords supported since 5.005
      (ABSTRACT	=> 'CPAN Testers Release database generator',
       AUTHOR	=> 'Barbie <barbie@cpan.org>') : ()),
);
