
######################################################################
## File: $Id: File.pm,v 1.1 2003/06/27 18:39:37 spadkins Exp $
######################################################################

use App;
use App::Repository;

package App::Repository::File;
$VERSION = do { my @r=(q$Revision: 1.1 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r};

@ISA = ( "App::Repository" );

use Data::Dumper;

use strict;

=head1 NAME

App::Repository::File - a repository which stores its data in flat files

=head1 SYNOPSIS

   use App::Repository::File;

   (see man page for App::Repository for additional methods)

   $rep = App::Repository::File->new();        # looks for %ENV, then config file

   $errmsg = $rep->error();       # returns the error string for prev op ("" if no error)
   $numrows = $rep->numrows();    # returns the number of rows affected by prev op
   print $rep->error(), "\n" if (!$rep->_connect());

   $value  = $rep->get ($table, $key,     $col,   \%options);
   $value  = $rep->get ($table, \%params, $col,   \%options);
   @row    = $rep->get ($table, $key,     \@cols, \%options);
   @row    = $rep->get ($table, \%params, \@cols, \%options);

   $nrows = $rep->set($table, $key,     $col,   $value,    \%options);
   $nrows = $rep->set($table, \%params, $col,   $value,    \%options);

   $row    = $rep->get_row ($table, $key,     \@cols, \%options);
   $row    = $rep->get_row ($table, \%params, \@cols, \%options);

   $nrows = $rep->set_row($table, $key,     \@cols, $row, \%options);
   $nrows = $rep->set_row($table, \%params, \@cols, $row, \%options);
   $nrows = $rep->set_row($table, undef,    \@cols, $row, \%options);

   $colvalues = $rep->get_column ($table, \%params, $col, \%options);

   $rows = $rep->get_rows ($table, \%params, \@cols, \%options);
   $rows = $rep->get_rows ($table, \%params, $col,   \%options);
   $rows = $rep->get_rows ($table, \@keys,   \@cols, \%options);

   $nrows = $rep->set_rows($table, \%params, \@cols, $rows, \%options);
   $nrows = $rep->set_rows($table, undef,    \@cols, $rows, \%options);
   $nrows = $rep->set_rows($table, \@keys,   \@cols, $rows, \%options);

   $values = $rep->get_values ($table, $key,     \@cols, \%options);
   $values = $rep->get_values ($table, \%params, \@cols, \%options);
   $values = $rep->get_values ($table, $key,     undef,  \%options);
   $values = $rep->get_values ($table, \%params, undef,  \%options);

   $values_list = $rep->get_values_list ($table, $key,     \@cols, \%options);
   $values_list = $rep->get_values_list ($table, \%params, \@cols, \%options);
   $values_list = $rep->get_values_list ($table, $key,     undef,  \%options);
   $values_list = $rep->get_values_list ($table, \%params, undef,  \%options);

   $nrows = $rep->set_values ($table, $key,     \@cols, $values, \%options);
   $nrows = $rep->set_values ($table, $key,     undef,  $values, \%options);
   $nrows = $rep->set_values ($table, undef,    \@cols, $values, \%options);
   $nrows = $rep->set_values ($table, undef,    undef,  $values, \%options);
   $nrows = $rep->set_values ($table, \%params, \@cols, $values, \%options);
   $nrows = $rep->set_values ($table, \%params, undef,  $values, \%options);

=cut

######################################################################
# ATTRIBUTES
######################################################################

# CONNECTION ATTRIBUTES
# $self->{dir}        # directory files are stored in

######################################################################
# INHERITED ATTRIBUTES
######################################################################

# BASIC
# $self->{name}       # name of this repository (often "db")

# CURRENT STATE
# $self->{error}      # most recent error generated from this module
# $self->{numrows}

# METADATA - Database Types
# $self->{types}
# $self->{type}{$type}{name}
# $self->{type}{$type}{num}
# $self->{type}{$type}{type}
# $self->{type}{$type}{column_size}
# $self->{type}{$type}{literal_prefix}
# $self->{type}{$type}{literal_suffix}
# $self->{type}{$type}{unsigned_attribute}
# $self->{type}{$type}{auto_unique_value}
# $self->{type}{$type}{quoted}

# METADATA - Tables and Columns
# $self->{table_names}
# $self->{table}{$table}{readonly}
# $self->{table}{$table}{columns}
# $self->{table}{$table}{column}{$column}
# $self->{table}{$table}{column}{$column}{name}
# $self->{table}{$table}{column}{$column}{type_name}
# $self->{table}{$table}{column}{$column}{type}
# $self->{table}{$table}{column}{$column}{notnull}
# $self->{table}{$table}{column}{$column}{quoted}

=head1 DESCRIPTION

The App::Repository::File class encapsulates all access to data stored
in flat files.  It provides an alternate data store to a database
for use with small datasets or in demonstration programs.

=cut

1;
