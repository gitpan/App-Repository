
######################################################################
## File: $Id: Makefile.PL 6704 2006-07-25 01:54:29Z spadkins $
######################################################################

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

%opts = (
    'NAME'        => 'App-Repository',
    'DISTNAME'    => 'App-Repository',
    'VERSION'     => '0.963',
    'PREREQ_PM'   => { #'App::Context' => 0,  # core services
                       'DBI'          => 0,  # database access
                     },
    'dist'        => {'COMPRESS'=>'gzip -9f', 'SUFFIX' => 'gz',
                      'ZIP'=>'/usr/bin/zip','ZIPFLAGS'=>'-rl'},
);

######################################################################
# MAKE THE MAKEFILE
######################################################################

WriteMakefile(%opts);

