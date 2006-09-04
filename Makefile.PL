
######################################################################
## File: $Id: Makefile.PL 6849 2006-09-04 19:20:19Z spadkins $
######################################################################

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

my @programs = (
    "bin/dbget",
);

%opts = (
    'NAME'        => 'App-Repository',
    'DISTNAME'    => 'App-Repository',
    'VERSION'     => '0.964',
    'EXE_FILES'   => [ @programs ],
    'PREREQ_PM'   => {
                       'App::Options' => 0,  # core services
                       'App::Context' => 0,  # core services
                       'DBI'          => "0.01",  # database access
                       'DBIx::Compat' => 0,       # database compatibility
                     },
);

######################################################################
# MAKE THE MAKEFILE
######################################################################

WriteMakefile(%opts);

