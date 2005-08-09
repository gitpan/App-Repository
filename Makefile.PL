
######################################################################
## File: $Id: Makefile.PL,v 1.8 2005/08/09 18:58:14 spadkins Exp $
######################################################################

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

%opts = (
    'NAME'        => 'App-Repository',
    'DISTNAME'    => 'App-Repository',
    'VERSION'     => '0.95',
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

