
#############################################################################
## $Id: Repository.pm 3523 2005-11-25 16:35:03Z spadkins $
#############################################################################

package App::Repository;

use App;
use App::Service;
@ISA = ( "App::Service" );

use strict;

use Date::Format;
use App::RepositoryObject;
use App::Reference;

=head1 NAME

App::Repository - a logical data access layer (works with App::Context), enabling an application to store and retrieve object data, relational data, and scalar data to any data repository such as databases, file systems, remote web sites, etc.

=head1 SYNOPSIS

    use App::Repository;

    $context = App->context();
    $repository = $context->service("Repository");  # or ...
    $repository = $context->repository();

    $rep = Repository::Base->new();        # looks for %ENV, then config file
    $rep = Repository::Base->new("sysdb"); # looks for %ENV, then config file using "sysdb"
    $rep2 = $rep->new();                              # copies attributes of existing $rep
    $rep = Repository::Base->new(@positional_args);   # undefined for Repository::Base
    $config = {
      'repository' => {
        'db' => {
          'arg1' => 'value1',
          'arg2' => 'value2',
        },
        'rep2' => {
          'arg1' => 'value1',
          'arg2' => 'value2',
        },
      },
    };
    $rep = Repository::Base->new($config);
    $rep = Repository::Base->new("rep2",$config);

    ###################################################################
    # The following methods are needed for SQL support
    ###################################################################

    $errmsg = $rep->error();       # returns the error string for prev op ("" if no error)
    $numrows = $rep->numrows();    # returns the number of rows affected by prev op
    print $rep->error(), "\n";

    # DATA TYPE HELPER METHODS
    $repdate = $rep->format_repdate($date_string);   # free-form date string as entered by a person
 
    # META-DATA: (about the tables)
    $rep->_load_rep_metadata();
    $rep->_load_table_metadata($tablename);
    $typenames    = $rep->get_type_names();                        # print "@$typenames\n";
    $typelabels   = $rep->get_type_labels();                       # print "%$typelabels\n";
    $typedef      = $rep->get_type_def($typename);                 # print "%$type\n";
    $tablenames   = $rep->get_table_names();                       # print "@$tablenames\n";
    $tablelabels  = $rep->get_table_labels();                      # print "%$tablelabels\n";
    $tabledef     = $rep->get_table_def($tablename);               # print "%$table\n";
    $columnnames  = $rep->get_column_names($tablename);            # print "@$columnnames\n";
    $columnlabels = $rep->get_column_labels($tablename);           # print "%$columnlabels\n";
    $columndef    = $rep->get_column_def($tablename,$columnname);  # print "%$column\n";

    #################################################
    # RELATIONAL
    #################################################

    ... (see App::Repository::DBI) ...

    $relation_names  = $rep->get_relation_names($table);
    $relation_labels = $rep->get_relation_labels($table);
    $relation_def    = $rep->get_relation_def($table, $relation_name);
    @keys            = $rep->get_related_keys($table, $key, $relation_name);

    #################################################
    # OBJECT-ORIENTED
    #################################################

    # OBJECT-ORIENTED
    $class = $table;
    $obj = $rep->object($class, $key);

    # OBJECT-ORIENTED (on RepositoryObject)
    $relation_names  = $obj->get_relation_names();
    $relation_labels = $obj->get_relation_labels();
    $relation_def    = $obj->get_relation_def($relation_name);
    @objs            = $obj->get_related_objects($relation_name);
 
    #################################################
    # TECHNICAL
    #################################################

    $rep->commit();
    $rep->rollback();
    $rep->import_rows($table, $file, $options);
    $rep->export_rows($table, $file, $options);

=cut

=head1 DESCRIPTION

A Repository is a means by which data may be stored somewhere or
retrieved from somewhere without
knowing what underlying technology is storing the data.

A Repository is the central persistence concept within the App.
A Repository does not present a uniquely object-oriented view of
its data.  Rather it presents a "logical relational" data model.
It does not return objects, but rows of data.

The "logical data model" means that a developer can program to
the data model which usually comes out of system requirements analysis,
closely modelling the business.  All of the changes to this
logical data model that are
incorporated during physical database design are abstracted
away, such as:

  * physical table naming,
  * physical column naming,
  * normalization of data into parent tables, and
  * splitting of tables based on various physical constraints.

This could be called object-to-relational mapping, but it is more
accurately called logical-to-physical-relational mapping.

Despite the fact that the Repository is a relational data storage
abstraction, persistent objects (i.e. RepositoryObjects) can be built to
save and restore their state from a Repository.  Furthermore, the
built-in support for non-scalar fields (references to arbitrarily
complex perl data structures) and the ability for RepositoryObjects
to encapsulate more than one row of data, makes the technology quite
fit for object-oriented development.

The design of the Repository is based around three important uses of
data.

  * Transaction Processing
  * Batch Processing
  * Report Generation

(more about this later)

The Repository abstraction seeks to solve the following problems.

  * objects may have attributes that come from multiple sources
  * caching
  * isolated from physical database changes
  * transactions
  * data source independence
  * no save/restore
  * devel/test/prod environments

What follows are some developing thoughts on this API...

  * The API should have two levels:
     = physical
       - no error-checking/defaults/security
       - provided by the driver
       - based on a physical table segment
       - application should never call this (private methods)
     = logical
       - error-checking
       - constraints (foreign key, check constraints)
       - column-level and row-level security
       - support transactions, caching, volatility
       - auditing

  * Isolation levels
     = do writers block readers, etc.

=cut

#############################################################################
# CLASS GROUP
#############################################################################

=head1 Class Group: Repository

The following classes might be a part of the Repository Class Group.

=over

=item * Class: App::Repository

=item * Class: App::Repository::DBI

=item * Class: App::Repository::File

=item * Class: App::Repository::BerkeleyDB

=item * Class: App::Repository::LDAP

=item * Class: App::Repository::HTML
      - for data stored in a web page

=item * Class: App::Repository::SOAP
      - remote data storage

=item * Class: App::Repository::Cache
      - use the Cache::Cache module

=item * Class: App::Repository::SPOPS
      - maybe?

=item * Class: App::Repository::Tangram
      - maybe?

=item * Class: App::Repository::Alzabo
      - maybe?

=item * Class: App::Repository::ClassDBI
      - maybe?

=back

=cut

#############################################################################
# CLASS
#############################################################################

=head1 Class: App::Repository

A Repository is a means by which data may be stored somewhere without
knowing what underlying technology is storing the data.

 * Throws: App::Exception::Repository
 * Since:  0.01

=head2 Class Design

...

=cut

#############################################################################
# CONSTANTS
#############################################################################

sub OK { 1; }

#############################################################################
# ATTRIBUTES
#############################################################################

# BASIC
# $self->{name}       # name of this repository (often "db")
# $self->{conf}       # hash of config file data

# CURRENT STATE
# $self->{error}      # most recent error generated from this module
# $self->{numrows}

# METADATA - Database Types
# $self->{types}
# $self->{type}{$type}
# $self->{type}{$typenum}
# $self->{type}{$type}{type_name}
# $self->{type}{$type}{data_type}
# $self->{type}{$type}{column_size}
# $self->{type}{$type}{literal_prefix}
# $self->{type}{$type}{literal_suffix}
# $self->{type}{$type}{unsigned_attribute}
# $self->{type}{$type}{auto_unique_value}
# $self->{type}{$type}{quoted}

# METADATA - Tables and Columns
# $self->{tables}
# $self->{table}{$table}{readonly}
# $self->{table}{$table}{columns}
# $self->{table}{$table}{column}{$column}
# $self->{table}{$table}{column}{$column}{name}
# $self->{table}{$table}{column}{$column}{type_name}
# $self->{table}{$table}{column}{$column}{type}
# $self->{table}{$table}{column}{$column}{notnull}
# $self->{table}{$table}{column}{$column}{quoted}

#############################################################################
# METHODS
#############################################################################

=head1 Methods

=cut

#############################################################################
# new()
#############################################################################

=head2 new()

The constructor is inherited from
L<C<App::Service>|App::Service/"new()">.

=cut

#############################################################################
# _connect()
#############################################################################

=head2 _connect()

    * Signature: $repository->_connect();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $repository->_connect();

Connects to the repository.  Most repositories have some connection
initialization that takes time and therefore should be done once.
Then many operations may be executed against the repository.
Finally the connection to the repository is closed (_disconnect()).

The default implementation of _connect() does nothing.
It is intended to be overridden in the subclass (if necessary).

=cut

sub _connect { 1; }

#############################################################################
# _disconnect()
#############################################################################

=head2 _disconnect()

    * Signature: $repository->_disconnect();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $repository->_disconnect();

Disconnects from the repository.

The default implementation of _disconnect() does nothing.
It is intended to be overridden in the subclass (if necessary).

All implementations of _disconnect() by a subclass must be sensitive to
whether the object is actually currently connected to the repository.
Thus, _disconnect() should be callable without negative consequences
even when the repository is already disconnected.

=cut

sub _disconnect { 1; }

#############################################################################
# _is_connected()
#############################################################################

=head2 _is_connected()

    * Signature: $connected = $repository->_is_connected();
    * Param:     void
    * Return:    $connected         integer
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    if ($repository->_is_connected()) {
        ...
    }

Reports whether a connection currently exists to the repository.

The default implementation of _is_connected() returns true (1) always.
It is intended to be overridden in the subclass (if necessary).

=cut

sub _is_connected { 1; }

#############################################################################
# PUBLIC METHODS
#############################################################################

=head1 Public Methods

=cut

#############################################################################
# error()
#############################################################################

=head2 error()

    * Signature: $errormsg = $repository->error();
    * Param:     void
    * Return:    $errormsg          string
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    print $repository->error(), "\n";

Returns the error string associated with the last operation
(or "" if there was no error).

The default implementation of error() simply returns the attribute {error}
which must be cleared at the beginning of every operation and set when
appropriate.

It is intended to be overridden in the subclass (if necessary).

=cut

sub error {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;
    my $error = $self->{error} || "";
    &App::sub_exit($error) if ($App::trace);
    return $error;
}

#############################################################################
# numrows()
#############################################################################

=head2 numrows()

    * Signature: $nrows = $repository->numrows();
    * Param:     void
    * Return:    $numrows           integer
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $nrows = $repository->numrows();

Returns the number of rows affected by the last operation.

The default implementation of numrows() simply returns the attribute {numrows}
which must be set to 0 at the beginning of every operation and set to a 
higher number when appropriate.

It is intended to be overridden in the subclass (if necessary).

=cut

sub numrows {
    return( $_[0]->{numrows} || 0 );
}

#############################################################################
# get()
#############################################################################

=head2 get()

    * Signature: $value = $rep->get ($table, $key,    $col,  $options); [tbd]
    * Signature: $value = $rep->get ($table, $params, $col,  $options); [tbd]
    * Signature: @row   = $rep->get ($table, $key,    $cols, $options); [tbd]
    * Signature: @row   = $rep->get ($table, $params, $cols, $options); [tbd]
    * Param:     $table     string
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $col       string
    * Param:     $cols      ARRAY
    * Param:     $options   undef,HASH
    * Return:    $value     any
    * Return:    @row       any
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $value  = $rep->get($table, $key,     $col,   \%options);
    $value  = $rep->get($table, \%params, $col,   \%options);
    @row    = $rep->get($table, $key,     \@cols, \%options);
    @row    = $rep->get($table, \%params, \@cols, \%options);

tbd.

=cut

sub get {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    die "get(): params undefined" if (!defined $params);
    my ($row, $wantarray);
    if (ref($cols) eq "ARRAY") {
        $wantarray = 1;
    }
    else {
        $cols = [ $cols ];
        $wantarray = 0;
    }
    $row = $self->get_row($table, $params, $cols, $options);
    if (!$row) {
        &App::sub_exit(undef) if ($App::trace);
        return(undef);
    }
    elsif ($wantarray) {
        &App::sub_exit(@$row) if ($App::trace);
        return(@$row);
    }
    else {
        &App::sub_exit($row->[0]) if ($App::trace);
        return($row->[0]);
    }
}

#############################################################################
# set()
#############################################################################

=head2 set()

    * Signature: $nrows = $rep->set($table, $key,    $col, $value, $options); [tbd]
    * Signature: $nrows = $rep->set($table, $params, $col, $value, $options); [tbd]
    * Param:     $table     string
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $col       string
    * Param:     $value     any
    * Param:     $options   undef,HASH
    * Return:    $nrows     integer
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $nrows = $rep->set($table, $key,     $col, $value, \%options);
    $nrows = $rep->set($table, \%params, $col, $value, \%options);

tbd.

=cut

sub set {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $col, $value, $options) = @_;
    die "set(): params undefined" if (!defined $params);
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    my ($nrows);
    if ($col && ref($col) eq "") {
        $nrows = $self->set_row($table, $params, [$col], [$value], $options);
    }
    else {
        $nrows = $self->set_row($table, $params, $col, $value, $options);
    }
    &App::sub_exit($nrows) if ($App::trace);
    return($nrows);
}

#############################################################################
# get_row()
#############################################################################

=head2 get_row()

    * Signature: $row = $rep->get_row ($table, $key,    $cols, $options);
    * Signature: $row = $rep->get_row ($table, $params, $cols, $options);
    * Param:     $table     string
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $cols      ARRAY
    * Param:     $options   undef,HASH
    * Return:    $row       ARRAY
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $row = $rep->get_row($table, $key,     \@cols, \%options);
    $row = $rep->get_row($table, \%params, \@cols, \%options);

tbd.

=cut

sub get_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    die "get_row(): params undefined" if (!defined $params);

    my ($row);
    my $repname = $self->{table}{$table}{repository};
    if (defined $repname && $repname ne $self->{name}) {
        my $rep = $self->{context}->repository($repname);
        $row = $rep->get_row($table, $params, $cols, $options);
    }
    else {
        $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
        if (!defined $cols) {
            $cols = $self->_get_default_columns($table);
        }
        elsif (!ref($cols)) {
            $cols = [ $cols ];
        }
        elsif ($#$cols == -1) {
            my $columns = $self->_get_default_columns($table);
            @$cols = @$columns;
        }

        my ($col, $contains_expr);
        my $column_defs = $self->{table}{$table}{column};
        for (my $i = 0; $i <= $#$cols; $i++) {
            $col = $cols->[$i];
            $contains_expr = 1 if ($column_defs->{$col}{expr});
            # TO BE IMPLEMENTED: Automatically follow relationships for column defs
            # TO BE IMPLEMENTED: Delegated get_rows() and merge on another table
            #for ($rel = 0; $rel <= $#rel_prefix; $rel++) {
            #    $rel_prefix  = $rel_prefix[$rel];
            #    $rel_cols    = $rel_cols[$rel];
            #    $rel_col_idx = $rel_col_idx[$rel];
            #    if ($col =~ /^${rel_prefix}_(.+)$/) {
            #        $col2 = $1;
            #        push(@$rel_cols, $col2);
            #        $rel_col_idx->[$#$rel_cols] = $i;
            #        last;
            #    }
            #}
        }
        if ($contains_expr) {
            $cols = $self->extend_columns($table, $cols);
        }

        $row = $self->_get_row($table, $params, $cols, $options);

        if ($contains_expr) {
            $self->evaluate_expressions($table, $params, $cols, [$row], $options);
        }
    }
    &App::sub_exit($row) if ($App::trace);
    return($row);
}

#############################################################################
# set_row()
#############################################################################

=head2 set_row()

    * Signature: $nrows = $rep->set_row($table, $key,    $cols, $row, $options);
    * Signature: $nrows = $rep->set_row($table, $params, $cols, $row, $options);
    * Signature: $nrows = $rep->set_row($table, $params, $cols, $rowhash, $options);
    * Signature: $nrows = $rep->set_row($table, $hash,   undef, undef,$options);
    * Signature: $nrows = $rep->set_row($table, $params, $hash, undef,$options);
    * Param:     $table     string
    * Param:     $cols      ARRAY
    * Param:     $row       ARRAY
    * Param:     $rowhash   HASH
    * Param:     $key       string
    * Param:     $hash      HASH
    * Param:     $params    undef,HASH
    * Param:     $options   undef,HASH
    * Return:    $nrows     integer
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $nrows = $rep->set_row($table, $key,     \@cols, $row, \%options);
    $nrows = $rep->set_row($table, \%params, \@cols, $row, \%options);
    $nrows = $rep->set_row($table, undef,    \@cols, $row, \%options);

tbd.

=cut

sub set_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    die "set_row(): params undefined" if (!defined $params);
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});

    my ($nrows, $key_defined);
    if ($row) {
        my $ref = ref($row);
        if ($ref && $ref ne "ARRAY") {
            $row = [ @{$row}{@$cols} ];
        }
        $nrows = $self->_set_row($table, $params, $cols, $row, $options);
    }
    else {
        my ($hash, $columns);
        if ($cols) {
            $hash = $cols;
            my $tabledef = $self->get_table_def($table);
            $columns = $tabledef->{columns};
            $columns = [ keys %$hash ] if (!$columns);
        }
        else {
            $hash = $params;     # a hashref was passed in instead of cols/row
            my $tabledef = $self->get_table_def($table);
            $columns = $tabledef->{columns};
            $columns = [ keys %$hash ] if (!$columns);
            $params = undef;
        }

        my (@cols, @row);
        foreach my $col (@$columns) {
            if (exists $hash->{$col}) {
                push(@cols, $col);
                push(@row, $hash->{$col});
            }
        }

        $key_defined = 1;

        if (!defined $params) {
            my $primary_key = $self->{table}{$table}{primary_key};
            $primary_key = [$primary_key] if (ref($primary_key) eq "");
            $params = {};
            my ($col);
            for (my $keypos = 0; $keypos <= $#$primary_key; $keypos++) {
                $col = $primary_key->[$keypos];
                if (defined $hash->{$col}) {
                    $params->{$col} = $hash->{$col};
                }
                else {
                    $key_defined = 0;
                    last;
                }
            }
        }

        if ($key_defined) {
            $nrows = $self->_set_row($table, $params, \@cols, \@row, $options);
        }
        else {
            $nrows = 0;
        }
    }

    &App::sub_exit($nrows) if ($App::trace);
    return($nrows);
}

#############################################################################
# get_column()
#############################################################################

=head2 get_column()

    * Signature: $colvalues = $rep->get_column ($table, $params, $col, $options);
    * Param:     $table     string
    * Param:     $params    undef,HASH
    * Param:     $col       string
    * Param:     $options   undef,HASH
    * Return:    $colvalues ARRAY
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $colvalues = $rep->get_column ($table, \%params, $col, \%options);

tbd.

=cut

sub get_column {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $col, $options) = @_;
    my (@colvalues, $rows, $row);
    @colvalues = ();
    $rows = $self->get_rows($table, $params, $col, $options);
    foreach $row (@$rows) {
        push(@colvalues, $row->[0]) if ($row && $#$row >= 0);
    }
    &App::sub_exit(\@colvalues) if ($App::trace);
    return(\@colvalues);
}

#############################################################################
# get_rows()
#############################################################################

=head2 get_rows()

    * Signature: $rows = $rep->get_rows($table, $params, $cols, $options);
    * Signature: $rows = $rep->get_rows($table, $keys,   $cols, $options);
    * Param:     $table     string
    * Param:     $params    undef,HASH
    * Param:     $keys      ARRAY
    * Param:     $cols      ARRAY
    * Param:     $options   undef,HASH
    * Return:    $rows      ARRAY
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $rows = $rep->get_rows ($table, \%params, \@cols, \%options);
    $rows = $rep->get_rows ($table, \%params, $col,   \%options);
    $rows = $rep->get_rows ($table, \@keys,   \@cols, \%options);

tbd.

=cut

sub get_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    my ($rows);
    my $repname = $self->{table}{$table}{repository};
    my $realtable = $self->{table}{$table}{table} || $table;
    if (defined $repname && $repname ne $self->{name}) {
        my $rep = $self->{context}->repository($repname);
        $rows = $rep->get_rows($realtable, $params, $cols, $options);
    }
    elsif (defined $realtable && $realtable ne $table) {
        $rows = $self->get_rows($realtable, $params, $cols, $options);
    }
    else {
        $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
        if (!defined $cols) {
            $cols = $self->_get_default_columns($table);
        }
        elsif (!ref($cols)) {
            $cols = [ $cols ];
        }
        elsif ($#$cols == -1) {
            my $columns = $self->_get_default_columns($table);
            @$cols = @$columns;
        }

        my ($col, $contains_expr);
        my $column_defs = $self->{table}{$table}{column};
        for (my $i = 0; $i <= $#$cols; $i++) {
            $col = $cols->[$i];
            $contains_expr = 1 if ($column_defs->{$col}{expr});
            # TO BE IMPLEMENTED: Automatically follow relationships for column defs
            # TO BE IMPLEMENTED: Delegated get_rows() and merge on another table
            #for ($rel = 0; $rel <= $#rel_prefix; $rel++) {
            #    $rel_prefix  = $rel_prefix[$rel];
            #    $rel_cols    = $rel_cols[$rel];
            #    $rel_col_idx = $rel_col_idx[$rel];
            #    if ($col =~ /^${rel_prefix}_(.+)$/) {
            #        $col2 = $1;
            #        push(@$rel_cols, $col2);
            #        $rel_col_idx->[$#$rel_cols] = $i;
            #        last;
            #    }
            #}
        }
        if ($contains_expr) {
            $cols = $self->extend_columns($table, $cols);
        }

        $rows = $self->_get_rows($table, $params, $cols, $options);

        if ($contains_expr) {
            $self->evaluate_expressions($table, $params, $cols, $rows, $options);
        }
    }
    &App::sub_exit($rows) if ($App::trace);
    return($rows);
}

sub _get_default_columns {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;
    my $table_def = $self->{table}{$table};
    my $columns = $table_def->{default_columns} || $table_def->{columns};
    $columns = $table_def->{columns} if ($columns eq "configured");
    die "Unknown default columns [$columns]" if (ref($columns) ne "ARRAY");
    &App::sub_exit($columns) if ($App::trace);
    return($columns);
}

sub extend_columns {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols) = @_;
    my (%colidx, $expr_columns, $expr, $extended, $col);
    for (my $i = 0; $i <= $#$cols; $i++) {
        $col = $cols->[$i];
        $colidx{$col} = $i;
    }
    my $column_defs = $self->{table}{$table}{column};
    for (my $i = 0; $i <= $#$cols; $i++) {
        $col = $cols->[$i];
        if ($column_defs->{$col}{expr_columns}) {
            $expr_columns = $column_defs->{$col}{expr_columns};
        }
        elsif ($column_defs->{$col}{expr}) {
            $expr = $column_defs->{$col}{expr};
            $expr =~ s/^[^\{\}]*\{//;
            $expr =~ s/\}[^\{\}]*$//;
            $expr_columns = [ split(/\}[^\{\}]*\{/, $expr) ];
            $column_defs->{$col}{expr_columns} = $expr_columns;
        }
        else {
            next;
        }
        foreach my $expr_col (@$expr_columns) {
            if (! defined $colidx{$expr_col}) {
                if (!$extended) {
                    $cols = [ @$cols ];  # make a copy. don't extend original.
                    $extended = 1;
                }
                push(@$cols, $expr_col);
                $colidx{$expr_col} = $#$cols;
            }
        }
    }
    &App::sub_exit($cols) if ($App::trace);
    return($cols);
}

#############################################################################
# set_rows()
#############################################################################

=head2 set_rows()

    * Signature: $nrows = $rep->set_rows($table, $keys, $cols, $rows, $options);
    * Param:     $table     string
    * Param:     $keys      undef,ARRAY
    * Param:     $cols      ARRAY
    * Param:     $rows      ARRAY
    * Param:     $options   undef,HASH
    * Return:    $nrows     integer
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $nrows = $rep->set_rows($table, \%params, \@cols, $rows, \%options);
    $nrows = $rep->set_rows($table, undef,    \@cols, $rows, \%options);
    $nrows = $rep->set_rows($table, \@keys,   \@cols, $rows, \%options);

tbd.

=cut

sub set_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $rows, $options) = @_;
    die "set_rows(): params undefined" if (!defined $params);
    my ($nrows);
    my $repname = $self->{table}{$table}{repository};
    my $realtable = $self->{table}{$table}{table} || $table;
    if (defined $repname && $repname ne $self->{name}) {
        my $rep = $self->{context}->repository($repname);
        $nrows = $rep->set_rows($realtable, $params, $cols, $rows, $options);
    }
    elsif (defined $realtable && $realtable ne $table) {
        $nrows = $self->set_rows($realtable, $params, $cols, $rows, $options);
    }
    else {
        $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
        $nrows = $self->_set_rows($table, $params, $cols, $rows, $options);
    }
    &App::sub_exit($nrows) if ($App::trace);
    return($nrows);
}

#############################################################################
# get_hash()
#############################################################################

=head2 get_hash()

    * Signature: $values = $rep->get_hash ($table, $key,    $cols, $options);
    * Signature: $values = $rep->get_hash ($table, $params, $cols, $options);
    * Param:     $table     string
    * Param:     $cols      ARRAY,undef
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $options   undef,HASH
    * Return:    $values    HASH
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $values = $rep->get_hash ($table, $key,     \@cols,   \%options);
    $values = $rep->get_hash ($table, \%params, \@cols,   \%options);
    $values = $rep->get_hash ($table, $key,     undef,    \%options);
    $values = $rep->get_hash ($table, \%params, undef,    \%options);

tbd.

=cut

sub get_hash {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    die "get_hash(): params undefined" if (!defined $params);
    $cols = [] if (!$cols);
    my $row = $self->get_row($table, $params, $cols, $options);
    my ($hash, $col, $value);
    if ($row && $#$row > -1) {
        $hash = {};
        for (my $idx = 0; $idx <= $#$cols; $idx++) {
            $col = $cols->[$idx];
            $value = $row->[$idx];
            $hash->{$col} = $value;
        }
    }
    &App::sub_exit($hash) if ($App::trace);
    return($hash);
}

#############################################################################
# get_hashes()
#############################################################################

=head2 get_hashes()

    * Signature: $hashes = $rep->get_hashes ($table, $key,    $cols, $options);
    * Signature: $hashes = $rep->get_hashes ($table, $params, $cols, $options);
    * Param:     $table        string
    * Param:     $cols         ARRAY,undef
    * Param:     $key          string
    * Param:     $params       undef,HASH
    * Param:     $options      undef,HASH
    * Return:    $hashes       ARRAY
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $hashes = $rep->get_hashes ($table, $key,     \@cols,   \%options);
    $hashes = $rep->get_hashes ($table, \%params, \@cols,   \%options);
    $hashes = $rep->get_hashes ($table, $key,     undef,    \%options);
    $hashes = $rep->get_hashes ($table, \%params, undef,    \%options);

tbd.

=cut

sub get_hashes {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    $cols = [] if (!$cols);
    my $rows = $self->get_rows($table, $params, $cols, $options);
    my $hashes = [];
    my ($hash, $row, $col, $value);
    if ($rows && $#$rows > -1) {
        foreach $row (@$rows) {
            $hash = {};
            for (my $idx = 0; $idx <= $#$cols; $idx++) {
                $col = $cols->[$idx];
                $value = $row->[$idx];
                $hash->{$col} = $value;
            }
            push(@$hashes, $hash);
        }
    }
    &App::sub_exit($hashes) if ($App::trace);
    return($hashes);
}

#############################################################################
# get_object()
#############################################################################

=head2 get_object()

    * Signature: $object = $rep->get_object ($table, $key,    $cols, $options);
    * Signature: $object = $rep->get_object ($table, $params, $cols, $options);
    * Param:     $table     string
    * Param:     $cols      ARRAY,undef
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $options   undef,HASH
    * Return:    $object    App::RepositoryObject
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $object = $rep->get_object ($table, $key,     \@cols,   \%options);
    $object = $rep->get_object ($table, \%params, \@cols,   \%options);
    $object = $rep->get_object ($table, $key,     undef,    \%options);
    $object = $rep->get_object ($table, \%params, undef,    \%options);

tbd.

=cut

sub get_object {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    die "get_object(): params undefined" if (!defined $params);
    my $tabledef = $self->{table}{$table};
    my $class = $tabledef->{class} || "App::RepositoryObject";
    App->use($class);
    my ($object);
    if (ref($cols) eq "ARRAY" && $#$cols == -1 && !ref($params)) {
        $object = {};
    }
    else {
        $object = $self->get_hash($table, $params, $cols, $options);
    }
    if ($object) {
        $object->{_repository} = $self;
        $object->{_table} = $table;
        bless $object, $class;
        if (!ref($params)) {
            $object->{_key} = $params;
        }
        else {
            my $primary_key = $tabledef->{primary_key};
            $primary_key = [$primary_key] if (ref($primary_key) eq "");
            my ($key);
            if ($primary_key) {
                $key = undef;
                foreach my $column (@$primary_key) {
                    if (defined $object->{$column}) {
                        if (defined $key) {
                            $key .= "," . $object->{$column};
                        }
                        else {
                            $key = $object->{$column};
                        }
                    }
                    else {
                        $key = undef;
                        last;
                    }
                }
                $object->{_key} = $key if (defined $key);
            }
        }
    }
    &App::sub_exit($object) if ($App::trace);
    return($object);
}

#############################################################################
# get_objects()
#############################################################################

=head2 get_objects()

    * Signature: $objects = $rep->get_objects ($table, $key,    $cols, $options);
    * Signature: $objects = $rep->get_objects ($table, $params, $cols, $options);
    * Param:     $table        string
    * Param:     $cols         ARRAY,undef
    * Param:     $key          string
    * Param:     $params       undef,HASH
    * Param:     $options      undef,HASH
    * Return:    $objects      ARRAY
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $objects = $rep->get_objects ($table, $key,     \@cols,   \%options);
    $objects = $rep->get_objects ($table, \%params, \@cols,   \%options);
    $objects = $rep->get_objects ($table, $key,     undef,    \%options);
    $objects = $rep->get_objects ($table, \%params, undef,    \%options);

tbd.

=cut

sub get_objects {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    my $tabledef = $self->{table}{$table};
    my $class = $tabledef->{class} || "App::RepositoryObject";
    App->use($class);
    my $objects = $self->get_hashes($table, $params, $cols, $options);
    my $primary_key = $tabledef->{primary_key};
    $primary_key = [$primary_key] if (ref($primary_key) eq "");
    my ($key);
    foreach my $object (@$objects) {
        $object->{_repository} = $self;
        $object->{_table} = $table;
        bless $object, $class;
        if ($primary_key) {
            $key = undef;
            foreach my $column (@$primary_key) {
                if (defined $object->{$column}) {
                    if (defined $key) {
                        $key .= "," . $object->{$column};
                    }
                    else {
                        $key = $object->{$column};
                    }
                }
                else {
                    $key = undef;
                    last;
                }
            }
            $object->{_key} = $key if (defined $key);
        }
    }
    &App::sub_exit($objects) if ($App::trace);
    return($objects);
}

#############################################################################
# get_hash_of_values_by_key()
#############################################################################

=head2 get_hash_of_values_by_key()

    * Signature: $hashes = $rep->get_hash_of_values_by_key ($table, $params, $valuecol, $keycol, $options);
    * Param:     $table        string
    * Param:     $params       undef,HASH
    * Param:     $valuecol     string
    * Param:     $keycol       string
    * Param:     $options      undef,HASH
    * Return:    $hash         HASH
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $hash = $rep->get_hash_of_values_by_key ($table, \%params, $valuecol, $keycol, \%options);

tbd.

=cut

sub get_hash_of_values_by_key {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $valuecol, $keycol, $options) = @_;
    my $rows = $self->get_rows($table, $params, [$keycol, $valuecol], $options);
    my $hash = {};
    if ($rows && $#$rows > -1) {
        foreach my $row (@$rows) {
            $hash->{$row->[0]} = $row->[1];
        }
    }
    &App::sub_exit($hash) if ($App::trace);
    return($hash);
}

#############################################################################
# get_hash_of_hashes_by_key()
#############################################################################

=head2 get_hash_of_hashes_by_key()

    * Signature: $hashes = $rep->get_hash_of_hashes_by_key ($table, $params, $cols, $keycol, $options);
    * Param:     $table        string
    * Param:     $params       undef,HASH
    * Param:     $cols         ARRAY
    * Param:     $keycol       string
    * Param:     $options      undef,HASH
    * Return:    $hash         HASH
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $hash = $rep->get_hash_of_hashes_by_key ($table, \%params, $cols, $keycol, \%options);

tbd.

=cut

sub get_hash_of_hashes_by_key {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $keycol, $options) = @_;
    my $hashes = $self->get_hashes($table, $params, $cols, $options);
    my $hash_of_hashes = {};
    if ($hashes && $#$hashes > -1) {
        foreach my $hash (@$hashes) {
            $hash_of_hashes->{$hash->{$keycol}} = $hash;
        }
    }
    &App::sub_exit($hash_of_hashes) if ($App::trace);
    return($hash_of_hashes);
}

#############################################################################
# set_hash()
#############################################################################

=head2 set_hash()

    * Signature: $nrows = $rep->set_hash ($table, $key,    $cols, $values, $options);
    * Signature: $nrows = $rep->set_hash ($table, $params, $cols, $values, $options);
    * Param:     $table     string
    * Param:     $key       string
    * Param:     $params    undef,HASH
    * Param:     $cols      ARRAY,undef
    * Param:     $options   undef,HASH
    * Return:    $nrows     integer
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $nrows = $rep->set_hash ($table, $key,     \@cols, $values, \%options);
    $nrows = $rep->set_hash ($table, $key,     undef,  $values, \%options);
    $nrows = $rep->set_hash ($table, undef,    \@cols, $values, \%options);
    $nrows = $rep->set_hash ($table, undef,    undef,  $values, \%options);
    $nrows = $rep->set_hash ($table, \%params, \@cols, $values, \%options);
    $nrows = $rep->set_hash ($table, \%params, undef,  $values, \%options);

tbd.

=cut

sub set_hash {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $values, $options) = @_;
    die "set_hash(): params undefined" if (!defined $params);
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    &App::sub_exit() if ($App::trace);
}

sub _params_to_hashref {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params) = @_;

    if (!defined $params || $params eq "") {
        $params = {};
    }
    elsif (!ref($params)) {
        $params = $self->_key_to_params($table,$params);  # $params is undef/scalar => $key
    }

    &App::sub_exit($params) if ($App::trace);
    return($params);
}

sub _row_matches {
    &App::sub_entry if ($App::trace);
    my ($self, $row, $table, $params, $cols, $options) = @_;

    $options = {} if (!$options);

    my ($tabledef, $param, $column, $repop, $colidxs, $colidx, $colvalue, $paramvalue);

    $colidxs = $options->{cache}{colidx};
    if (!defined $colidxs || ! %$colidxs) {
        my $columns = $self->{table}{$table}{columns};
        die "Columns not defined for table $table" if (!$columns);
        if (!defined $colidxs) {
            $colidxs = {};
            $options->{cache}{colidx} = $colidxs;
        }
        for ($colidx = 0; $colidx < $#$columns; $colidx++) {
            $column = $columns->[$colidx];
            $colidxs->{$column} = $colidx;
        }
    }

    my ($all_params_match, $param_match);
    $all_params_match = 1;      # assume it matches

    $tabledef = $self->{table}{$table};
    foreach $param (keys %$params) {
        $param_match = undef;
        $column = $param;
        $colidx = $colidxs->{$column};
        $colvalue = (defined $colidx) ? $row->[$colidx] : undef;
        $repop = "eq";
        # check if $column contains an embedded operation, i.e. "name.eq", "name.contains"
        if ($param =~ /^(.*)\.([^.]+)$/) {
            $column = $1;
            $repop = $2;
        }

        if (!defined $tabledef->{column}{$column}) {
            if ($param =~ /^begin_(.*)/) {
                $column = $1;
                $repop = "ge";
            }
            elsif ($param =~ /^end_(.*)/) {
                $column = $1;
                $repop = "le";
            }
        }
        next if (!defined $tabledef->{column}{$column});  # skip if the column is unknown

        $paramvalue = $params->{$param};
        if (defined $paramvalue) {

            if ($repop eq "contains") {
                $param_match = ($colvalue !~ /$paramvalue/);
            }
            elsif ($repop eq "matches") {
                $paramvalue =~ s/\*/\.\*/g;
                $paramvalue =~ s/\?/\./g;
                $param_match = ($colvalue !~ /^$paramvalue$/);
            }
            elsif ($repop eq "in" || $repop eq "eq") {
                if ($paramvalue =~ /,/ && ! $tabledef->{param}{$param}{no_auto_in_param}) {
                    $param_match = (",$paramvalue," =~ /,$colvalue,/);
                }
                elsif ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue == $paramvalue);
                }
                else {
                    $param_match = ($colvalue eq $paramvalue);
                }
            }
            elsif ($repop eq "gt") {
                if ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue > $paramvalue);
                }
                else {
                    $param_match = ($colvalue gt $paramvalue);
                }
            }
            elsif ($repop eq "ge") {
                if ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue >= $paramvalue);
                }
                else {
                    $param_match = ($colvalue ge $paramvalue);
                }
            }
            elsif ($repop eq "lt") {
                if ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue < $paramvalue);
                }
                else {
                    $param_match = ($colvalue lt $paramvalue);
                }
            }
            elsif ($repop eq "le") {
                if ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue <= $paramvalue);
                }
                else {
                    $param_match = ($colvalue le $paramvalue);
                }
            }
            elsif ($repop eq "ne") {
                if ($paramvalue =~ /^-?[0-9]*\.?[0-9]*$/) {
                    $param_match = ($colvalue != $paramvalue);
                }
                else {
                    $param_match = ($colvalue ne $paramvalue);
                }
            }
            else {
                next;
            }
        }
        if (!$param_match) {
            $all_params_match = 0;
            last;
        }
    }

    &App::sub_exit($all_params_match) if ($App::trace);
    return($all_params_match);
}

sub _row_columns {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $row, $cols) = @_;

    my ($idx, $native_idx, $column, @newrow);
    $#newrow = $#$cols;  # preallocate
    my $tabledef = $self->{table}{$table};
    for ($idx = 0; $idx <= $#$cols; $idx++) {
        $column = $cols->[$idx];
        $native_idx = $tabledef->{column}{$column}{idx};
        $newrow[$idx] = (defined $native_idx) ? $row->[$native_idx] : undef;
    }

    &App::sub_exit(\@newrow) if ($App::trace);
    return(\@newrow);
}

sub _get_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    if (!$options) {
        $options = { startrow => 1, endrow => 1 };
    }
    elsif (! defined $options->{endrow}) {
        $options = { %$options };
        $options->{endrow} = $options->{startrow} || 1;
    }
    my $rows = $self->_get_rows($table, $params, $cols, $options);
    my ($row);
    $row = $rows->[0] if ($#$rows > -1);
    &App::sub_exit($row) if ($App::trace);
    return($row);
}

sub _get_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    my $all_columns = (!defined $cols);
    $cols = $self->{table}{$table}{columns} if ($all_columns);
    $params = $self->_params_to_hashref($table, $params) if (ref($params) ne "HASH");
    $options  = {} if (!$options);
    my $startrow = $options->{startrow} || 0;
    my $endrow   = $options->{endrow} || 0;

    my ($rows, $row, $matched_rows, $rownum);
    $rows = $self->{table}{$table}{data};
    $matched_rows = [];
    if ($rows && ref($rows) eq "ARRAY") {
        for ($rownum = 0; $rownum <= $#$rows; $rownum++) {
            next if ($startrow && $rownum < $startrow-1);
            last if ($endrow && $rownum >= $endrow);
            $row = $rows->[$rownum];
            if ($self->_row_matches($row, $table, $params, $cols, $options)) {
                push(@$matched_rows, $all_columns ? $row : $self->_row_columns($table, $row, $cols));
            }
        }
    }

    &App::sub_exit($matched_rows) if ($App::trace);
    return($matched_rows);
}

sub _set_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $rows, $options) = @_;
    $params = $self->_params_to_hashref($table, $params) if ($params && ref($params) ne "HASH");

    my $tabledef = $self->{table}{$table};

    my ($primary_key, @keycolidx, $keypos, %keypos, $keys_supplied);
    my ($row, $colidx, $nrows, $success);
    $nrows = 0;
    if (! defined $params) {
        $primary_key = $tabledef->{primary_key};
        $primary_key = [$primary_key] if (ref($primary_key) eq "");
        for ($keypos = 0; $keypos <= $#$primary_key; $keypos++) {
            $keypos{$primary_key->[$keypos]} = $keypos;
        }
        $keys_supplied = 0;
        for ($colidx = 0; $colidx <= $#$cols; $colidx++) {
            $keypos = $keypos{$cols->[$colidx]};
            if (defined $keypos) {
                $keycolidx[$keypos] = $colidx;
                $keys_supplied++;
            }
        }
        die "Tried to set_rows() and the primary key is not among the columns" if ($keys_supplied != $#$primary_key+1);
        foreach $row (@$rows) {
            $success = $self->_update($table, \@keycolidx, $cols, $row, $options);
            if ($success == 0 && $options->{create}) {
                $success = $self->_insert_row($table, $cols, $row, $options);
            }
            $nrows += $success;
        }
    }
    elsif (ref($params) eq "ARRAY") {
        # $curr_rows = $self->_get_rows($table, $params, $cols, $options);
    }
    else { # i.e. "HASH"
        # $curr_rows = $self->_get_rows($table, $params, $cols, $options);
    }
    &App::sub_exit($nrows) if ($App::trace);
    return($nrows);
}

sub _set_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    $options = {} if (!$options);

    $params = $self->_params_to_hashref($table, $params) if ($params && ref($params) ne "HASH");
    my $nrows = $self->_update($table, $params, $cols, $row, $options);
    if ($nrows == 0 && $options->{create}) {
        $nrows = $self->_insert_row($table, $cols, $row, $options);
    }

    &App::sub_exit($nrows) if ($App::trace);
    return($nrows);
}

sub _key_to_values {
    &App::sub_entry if ($App::trace);
    my ($self, $key) = @_;
    # TODO: eventually, I should handle escaping of "," and nonprintable data
    my @values = split(/,/, $key);
    &App::sub_exit(@values) if ($App::trace);
    return(@values);
}

sub _values_to_key {
    &App::sub_entry if ($App::trace);
    my ($self, @values) = @_;
    # TODO: eventually, I should handle unescaping of "," and nonprintable data
    my $retval = join(",",@values);
    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

sub _key_to_params {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $key) = @_;
    my %params = ();
    my $primary_key = $self->{table}{$table}{primary_key};
    die "ERROR: primary key is not defined for table [$table]\n   (configure attribute {Repository}{$self->{name}}{table}{$table}{primary_key})\n"
        if (!defined $primary_key);
    $primary_key = $primary_key->[0] if (ref($primary_key) eq "ARRAY" && $#$primary_key == 0);
    if (ref($primary_key)) {
        my ($colnum, @values);
        if (!defined $key || $key eq "") {
            for ($colnum = 0; $colnum <= $#$primary_key; $colnum++) {
                $params{$primary_key->[$colnum]} = undef;
            }
        }
        else {
            @values = $self->_key_to_values($key);
            die "ERROR: values [$key] do not match columns [" . join(",",@$primary_key) . "] in primary key"
                if ($#$primary_key != $#values);
            for ($colnum = 0; $colnum <= $#$primary_key; $colnum++) {
                $params{$primary_key->[$colnum]} = $values[$colnum];
            }
        }
        $params{"_order"} = $primary_key;
    }
    else {
        $params{$primary_key} = $key;
    }
    &App::sub_exit(\%params) if ($App::trace);
    return(\%params);
}

# $ok = $rep->insert_row ($table, \@cols, \@row);
# $ok = $rep->insert_row ($table, \%obj);
sub insert_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $options) = @_;
    my ($retval, $hash, $columns);
    my $ref = ref($cols);
    if ($ref && $ref ne "ARRAY") {
        $hash = $cols;     # a hashref was passed in instead of cols/row
        my $tabledef = $self->get_table_def($table);
        $columns = [];
        foreach my $col (@{$tabledef->{columns}}) {
            if (exists $hash->{$col}) {
                push(@$columns, $col);
            }
        }
    }
    elsif (ref($row) eq "HASH") {
        $hash = $row;
        if (ref($cols) eq "ARRAY") {
            $columns = $cols;
        }
        else {
            my $tabledef = $self->get_table_def($table);
            $columns = [];
            foreach my $col (@{$tabledef->{columns}}) {
                if (exists $hash->{$col}) {
                    push(@$columns, $col);
                }
            }
        }
    }
    if ($hash) {
        my (@cols, @row);
        foreach my $col (@$columns) {
            if (exists $hash->{$col}) {
                push(@cols, $col);
                push(@row, $hash->{$col});
            }
        }
        $retval = $self->_insert_row($table, \@cols, \@row, $options);
    }
    else {
        $retval = $self->_insert_row($table, $cols, $row, $options);
    }
    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

# NOTE: insert() is a synonym for insert_row()
sub insert {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $options) = @_;
    my $retval = $self->insert_row($table, $cols, $row, $options);
    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

# NOTE: This might be optimized somehow in the future so that I don't
# need to do a select after insert.  However, there might be defaults
# set in the database that I don't know about, and I want them to be
# reflected in the returned object.
# NOTE 2: Tables which have
# $object = $rep->new_object($table, \@cols, \@row);
# $object = $rep->new_object($table, \%obj_values);
# $object = $rep->new_object($table, $col, $value);
# $object = $rep->new_object($table);
sub new_object {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $options) = @_;

    my $tabledef = $self->{table}{$table};
    my $class = $tabledef->{class} || "App::RepositoryObject";

    my $ref = ref($cols);
    my ($object);
    if ($ref && $ref eq "ARRAY") {
        $object = {};
        for (my $i = 0; $i <= $#$cols; $i++) {
            $object->{$cols->[$i]} = $row->[$i];
        }
    }
    elsif ($ref) {
        $object = { %$cols };
    }
    elsif ($cols) {
        $object = { $cols => $row };
    }
    else {
        $object = {};
    }

    App->use($class);
    bless $object, $class;
    $object->_init();
    $self->_check_default_and_required_fields($object);

    if (!$options->{temp}) {
        my $retval = $self->insert_row($table, $object, undef, $options);
        die "new($table) unable to create a new row" if (!$retval);
        my $params = $self->_last_inserted_id();
        if (!$params) {
            $params = {};
            foreach my $col (keys %$object) {
                $params->{$col . ".eq"} = $object->{$col};
            }
        }
        $object = $self->get_object($table, $params, undef, $options);
    }

    &App::sub_exit($object) if ($App::trace);
    $object;
}

sub _check_default_and_required_fields {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $hash) = @_;
    my $tabledef = $self->{table}{$table};
    my $columns = $tabledef->{column};
    if ($columns) {
        foreach my $column (keys %$columns) {
            if (!defined $hash->{$column}) {
                if (defined $columns->{$column}{default}) {
                    $hash->{$column} = $columns->{$column}{default};
                }
                elsif (defined $columns->{$column}{not_null}) {
                    die "Illegal object value for $table: $column cannot be NULL (i.e. undef)";
                }
            }
        }
    }
    my $primary_key = $tabledef->{primary_key};
    if ($primary_key) {
        # Watch out for auto-generated primary keys. It's OK for them to be NULL.
        #if ($#$primary_key > 0) {
        #    foreach my $column (@$primary_key) {
        #        if (!defined $hash->{$column}) {
        #            die "Illegal object value for $table: $column cannot be NULL because it exists in the primary key";
        #        }
        #    }
        #}
    }
    my $alternate_keys = $tabledef->{alternate_key};
    if ($alternate_keys) {
        foreach my $alternate_key (@$alternate_keys) {
            foreach my $column (@$alternate_key) {
                if (!defined $hash->{$column}) {
                    die "Illegal object value for $table: $column cannot be NULL because it exists in an alternate key";
                }
            }
        }
    }
    &App::sub_exit() if ($App::trace);
}

sub _last_inserted_id {
    my ($self) = @_;
    return(undef);  # sorry. maybe some subclass will know how to do this.
}

# $nrows = $rep->insert_rows ($table, \@cols, \@rows);
sub insert_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $rows, $options) = @_;
    my ($nrows, $hashes, $hash, $columns);
    if (ref($cols) eq "ARRAY" && ref($cols->[0]) eq "HASH") {
        $hashes = $cols;     # an array of hashrefs was passed in instead of cols/rows
        $hash = $hashes->[0];
        my $tabledef = $self->get_table_def($table);
        $columns = $tabledef->{columns};
        $columns = [ keys %$hash ] if (!$columns);
    }
    elsif (ref($rows) eq "ARRAY" && ref($rows->[0]) eq "HASH") {
        $hashes = $rows;
        $hash = $hashes->[0];
        if (ref($cols) eq "ARRAY") {
            $columns = $cols;
        }
        else {
            my $tabledef = $self->get_table_def($table);
            $columns = $tabledef->{columns};
            $columns = [ keys %$hash ] if (!$columns);
        }
    }
    if ($hashes) {
        my (@cols, @rows, $col, $row);
        foreach $col (@$columns) {
            if (exists $hash->{$col}) {
                push(@cols, $col);
            }
        }
        foreach $hash (@$hashes) {
            $row = [];
            foreach $col (@cols) {
                push(@$row, $hash->{$col});
            }
            push(@rows, $row);
        }
        $nrows = $self->_insert_rows($table, \@cols, \@rows, $options);
    }
    else {
        $nrows = $self->_insert_rows($table, $cols, $rows, $options);
    }
    &App::sub_exit($nrows) if ($App::trace);
    $nrows;
}

sub delete {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    die "set_row(): params undefined" if (!defined $params);
    my $retval = $self->_delete($table,$params,$cols,$row,$options);
    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

sub update {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    die "update(): params undefined" if (!defined $params);
    my $retval = $self->_update($table,$params,$cols,$row,$options);
    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

sub _insert_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $options) = @_;
    $self->{error} = "";
    my $retval = 0;
    die "_insert_row(): not yet implemented";
    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

sub _insert_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $rows, $options) = @_;
    $self->{error} = "";
    my $retval = 0;
    die "_insert_rows(): not yet implemented";
    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

sub _delete {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;

    $self->{error} = "";
    my $retval = 0;
    die "_delete(): not yet implemented";

    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

# $nrows = $rep->_update($table, \%params,    \@cols, \@row, \%options);
# $nrows = $rep->_update($table, \@keycolidx, \@cols, \@row, \%options);
# $nrows = $rep->_update($table, \@paramcols, \@cols, \@row, \%options);
# $nrows = $rep->_update($table, $key,        \@cols, \@row, \%options);
# $nrows = $rep->_update($table, undef,       \@cols, \@row, \%options);
sub _update {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;

    $self->{error} = "";
    my $retval = 0;

    my $get_options = { cache => {}, };
    my $rows = $self->_get_rows($table, $params, undef, $get_options);
    my $colidxs = $get_options->{cache}{colidx};
    my ($idx, $colidx, $column, $tablerow);
    foreach $tablerow (@$rows) {
        for ($idx = 0; $idx <= $#$cols; $idx++) {
            $column = $cols->[$idx];
            $colidx = $colidxs->{$column};
            if (defined $colidx) {
                $tablerow->[$colidx] = $row->[$idx];
            }
        }
    }
    $retval = $#$rows + 1;

    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

#############################################################################
# format_repdate()
#############################################################################

=head2 format_repdate()

    * Signature: $date = $repository->format_repdate($freeform_date);
    * Param:     $freeform_date     string
    * Return:    $date              string
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    foreach $freeform_date ("1/2/01", "1-Jan-2003", "january 13, 2000",
            "2000/1/5", "15 jan 99") {
        print "$freeform_date: ", $rep->format_repdate($freeform_date), "\n";
    }

The format_repdate() method takes a free-form date string (such as a human
might type into a form field) using many varieties of upper and lower case,
punctuation, and ordering, and turns it into a date in canonical
YYYY-MM-DD form for storage in the repository.

=cut

#  $repdate = $rep->format_repdate($date_string);   # free-form date string as entered by a person
sub format_repdate {
    my ($self, $datetext) = @_;
    my ($monthtext, $mon, $day, $year, %mon, $date);
    if ($datetext =~ /\b([a-zA-Z]+)[- ]+([0-9]{1,2})[- ,]+([0-9]{2,4})\b/) {  # i.e. December 31, 1999, 9-march-01
        $monthtext = $1;
        $day = $2;
        $year = $3;
    }
    elsif ($datetext =~ /\b([0-9]{1,2})[- ]+([a-zA-Z]+)[- ]+([0-9]{2,4})\b/) {  # i.e. 31-Dec-1999, 9 march 01
        $day = $1;
        $monthtext = $2;
        $year = $3;
    }
    elsif ($datetext =~ /\b([0-9]{4})([0-9]{2})([0-9]{2})\b/) {     # i.e. 19991231, 20010309
        $year = $1;
        $mon = $2;
        $day = $3;
    }
    elsif ($datetext =~ m!\b([0-9]{4})[- /]+([0-9]{1,2})[- /]+([0-9]{1,2})\b!) { # i.e. 1999-12-31, 2001/3/09
        $year = $1;
        $mon = $2;
        $day = $3;
    }
    elsif ($datetext =~ m!\b([0-9]{1,2})[- /]+([0-9]{1,2})[- /]+([0-9]{2,4})\b!) {  # i.e. 12/31/1999, 3-9-01
        $mon = $1;
        $day = $2;
        $year = $3;
    }
    else {
        return("");
    }
    if ($monthtext) {
        if    ($monthtext =~ /^jan/i) { $mon =  1; }
        elsif ($monthtext =~ /^feb/i) { $mon =  2; }
        elsif ($monthtext =~ /^mar/i) { $mon =  3; }
        elsif ($monthtext =~ /^apr/i) { $mon =  4; }
        elsif ($monthtext =~ /^may/i) { $mon =  5; }
        elsif ($monthtext =~ /^jun/i) { $mon =  6; }
        elsif ($monthtext =~ /^jul/i) { $mon =  7; }
        elsif ($monthtext =~ /^aug/i) { $mon =  8; }
        elsif ($monthtext =~ /^sep/i) { $mon =  9; }
        elsif ($monthtext =~ /^oct/i) { $mon = 10; }
        elsif ($monthtext =~ /^nov/i) { $mon = 11; }
        elsif ($monthtext =~ /^dec/i) { $mon = 12; }
        else                          { return("");  }
    }
    if ($year < 0) { return(""); }
    elsif ($year < 50) { $year += 2000; }
    elsif ($year < 100) { $year += 1900; }
    elsif ($year < 1000) { return(""); }
    return("") if ($mon > 12);
    return("") if ($day > 31);
    sprintf("%04d-%02d-%02d",$year,$mon,$day);
}

#############################################################################
# get_type_names()
#############################################################################

=head2 get_type_names()

    * Signature: $typenames = $repository->get_type_names();
    * Param:     void
    * Return:    $typenames         []
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $typenames = $rep->get_type_names();
    print join(",", @$typenames), "\n";

Returns the standard set of type names for columns in a repository.
These are perl-friendly type names which are useful to do data validation.

    * string
    * text
    * integer
    * float
    * date
    * time
    * datetime
    * binary

=cut

sub get_type_names {
    my ($self) = @_;
    $self->{types};
}

#############################################################################
# get_type_labels()
#############################################################################

=head2 get_type_labels()

    * Signature: $typelabels = $repository->get_type_labels();
    * Param:     void
    * Return:    $typelabels        {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $typelabels = $rep->get_type_labels();
    foreach (sort keys %$typelabels) {
        print "$_ => $typelabels->{$_}\n";
    }

Returns a hash of all of the repository types and the labels
which should be used when displaying them to the user through
the user interface.

    * string   => "Characters"
    * text     => "Text"
    * integer  => "Integer"
    * float    => "Number"
    * date     => "Date"
    * time     => "Time"
    * datetime => "Date and Time"
    * binary   => "Binary Data"

=cut

sub get_type_labels {
    my ($self) = @_;
    $self->{type_labels};
}

#############################################################################
# get_type_def()
#############################################################################

=head2 get_type_def()

    * Signature: $typedef = $rep->get_type_def($typename);
    * Param:     $typename          string
    * Return:    $typedef           {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $typedef = $rep->get_type_def("string");
    print "$typedef->{name} $typedef->{label}\n";

Gets a reference to a "type definition", which allows you to access all
of the attributes of the requested type
(currently only "name" and "label").

=cut

sub get_type_def {
    my ($self, $type) = @_;
    $self->{type}{$type};
}

#############################################################################
# get_table_names()
#############################################################################

=head2 get_table_names()

    * Signature: $tablenames = $rep->get_table_names();
    * Param:     void
    * Return:    $tablenames        []
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $tablenames = $rep->get_table_names();
    print join(",", @$tablenames), "\n";

Returns the set of table names in the repository.

=cut

sub get_table_names {
    my ($self) = @_;
    $self->{tables};
}

#############################################################################
# get_table_labels()
#############################################################################

=head2 get_table_labels()

    * Signature: $tablelabels = $rep->get_table_labels();
    * Param:     void
    * Return:    $tablelabels       {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $tablelabels = $rep->get_table_labels();
    foreach (sort keys %$tablelabels) {
        print "$_ => $tablelabels->{$_}\n";
    }

Returns a hash of all of the tables and the labels
which should be used when displaying them to the user through
the user interface.

=cut

sub get_table_labels {
    my ($self) = @_;
    $self->{table_labels};
}

#############################################################################
# get_table_def()
#############################################################################

=head2 get_table_def()

    * Signature: $tabledef = $rep->get_table_def($tablename);
    * Param:     $tablename         string
    * Return:    $tabledef          {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $tabledef = $rep->get_table_def($tablename);
    print "$tabledef->{name} $tabledef->{label}\n";

Gets a reference to a "table definition", which allows you to access all
of the attributes of the requested table.
By default, this is only "name" and "label".
However, for various types of repositories, there may be additional
attributes for a table.

=cut

sub get_table_def {
    my ($self, $table) = @_;
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    $self->{table}{$table};
}

#############################################################################
# get_column_names()
#############################################################################

=head2 get_column_names()

    * Signature: $columnnames = $rep->get_column_names($tablename);
    * Param:     $tablename         string
    * Return:    $columnnames       []
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $columnnames = $rep->get_column_names($tablename);
    print join(",", @$columnnames), "\n";

Returns the set of column names for the requested table in a repository.

=cut

sub get_column_names {
    my ($self, $table) = @_;
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    $self->{table}{$table}{columns};
}

#############################################################################
# get_column_labels()
#############################################################################

=head2 get_column_labels()

    * Signature: $columnlabels = $rep->get_column_labels($tablename);
    * Param:     $tablename         string
    * Return:    $columnlabels      {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $columnlabels = $rep->get_column_labels($tablename);
    foreach (sort keys %$columnlabels) {
        print "$_ => $columnlabels->{$_}\n";
    }

Returns a hash of all of the column names and the labels
which should be used when displaying them to the user through
the user interface.

=cut

sub get_column_labels {
    my ($self, $table) = @_;
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    $self->{table}{$table}{column_labels};
}

#############################################################################
# get_column_def()
#############################################################################

=head2 get_column_def()

    * Signature: $columndef = $rep->get_column_def($tablename,$columnname);
    * Param:     $tablename         string
    * Param:     $columnname        string
    * Return:    $columndef         {}
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $columndef = $rep->get_column_def($tablename,$columnname);
    print "$columndef->{name} $columndef->{label} $columndef->{type}\n";

Gets a reference to a "column definition", which allows you to access all
of the attributes of the requested column.

By default, this is only "name", "label", and "type".
However, for various types of repositories, there may be additional
attributes for a column.

=cut

# $column = $rep->get_column_def($tablename,$columnname); # print "%$column\n";
sub get_column_def {
    my ($self, $table, $column) = @_;
    $self->_load_table_metadata($table) if (! defined $self->{table}{$table}{loaded});
    $self->{table}{$table}{column}{$column};
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Transaction Control

=cut

#############################################################################
# commit()
#############################################################################

=head2 commit()

    * Signature: $rep->commit();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $rep->commit();

=cut

sub commit {
    my $self = shift;
    my ($table, $rows, $rowidx, $rowchange, $change, $colref, $prikeyidx, $nrows);

    $nrows = 0;
    foreach $table (@{$self->{tables}}) {

        $rowchange = $self->{table}{$table}{cache}{rowchange};

        if ($rowchange && $#$rowchange > -1) {

            $prikeyidx = $self->{table}{$table}{prikeyidx};
            if (!$prikeyidx) {
                $self->{context}->add_message("Table '$table' not configured for updating ('prikey' not set in commit())");
                next;
            }

            $rows      = $self->{table}{$table}{cache}{rows};
            $colref    = $self->{table}{$table}{cache}{columns};

            for ($rowidx = 0; $rowidx <= $#$rows; $rowidx++) {
                $change = $rowchange->[$rowidx];
                next if (!defined $change);
                if ($change eq "U") {
                    $self->_update($table, $colref, $rows->[$rowidx], $prikeyidx);
                    $rowchange->[$rowidx] = "";
                    $nrows++;
                }
                elsif ($change eq "I") {
                    $self->insert_row($table, $colref, $rows->[$rowidx]);
                    $rowchange->[$rowidx] = "";
                    $nrows++;
                }
                if ($App::DEBUG && $self->{context}->dbg(7)) {
                    my $context = $self->{context};
                    $context->dbgprint("rep->commit(): [$self->{sql}]");
                    $context->dbgprint("    [", join("|",@{$rows->[$rowidx]}), "]");
                }
            }
        }
    }
    $self->{context}->dbgprint("rep->commit(): nrows=$nrows")
        if ($App::DEBUG && $self->{context}->dbg(2));
}

#############################################################################
# rollback()
#############################################################################

=head2 rollback()

    * Signature: $rep->rollback();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $rep->rollback();

=cut

sub rollback {
    my $self = shift;
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Import/Export Data From File

=cut

#############################################################################
# import_rows()
#############################################################################

=head2 import_rows()

    * Signature: $rep->import_rows($table, $file);
    * Signature: $rep->import_rows($table, $file, $options);
    * Param:     $table        string
    * Param:     $file         string
    * Param:     $options      named
    * Param:     columns       ARRAY     names of columns of the fields in the file
    * Param:     replace       boolean   rows should replace existing rows based on unique indexes
    * Param:     field_sep     char      character which separates the fields in the file (can by "\t")
    * Param:     field_quote   char      character which optionally encloses the fields in the file (i.e. '"')
    * Param:     field_escape  char      character which escapes the quote chars within quotes (i.e. "\")
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $rep->import_rows("usr","usr.dat");

    # root:x:0:0:root:/root:/bin/bash
    $rep->import_rows("usr", "/etc/passwd" ,{
        field_sep => ":",
        columns => [ "username", "password", "uid", "gid", "comment", "home_directory", "shell" ],
    });

=cut

sub import_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $file, $options) = @_;
    my $columns = $options->{columns} || $self->{table}{$table}{columns};
    my $field_sep = $options->{field_sep} || ",";
    my $field_quote = $options->{field_quote};
    my $field_escape = $options->{field_escape};

    open(App::Repository::DBI::FILE, "< $file") || die "Unable to open $file for reading: $!";
    my (@row, $quoted_field_regexp, $field_regexp);
    while (<App::Repository::DBI::FILE>) {
        chomp;
        if ($field_quote) {
            @row = ();
            # TODO: incorporate escaping
            $field_regexp        = "$field_sep?$field_quote([^$field_quote]*)$field_quote";
            $quoted_field_regexp = "$field_sep?([^$field_sep]*)";
            while ($_) {
                if ($_ =~ s/^$quoted_field_regexp//) {
                    push(@row, $1);
                }
                elsif ($_ =~ s/^$field_regexp//) {
                    push(@row, $1);
                }
                else {
                    die "Imported data doesn't match quoted or unquoted field [$_]";
                }
            }
        }
        else {
            @row = split(/$field_sep/);
        }
        # TODO: use insert_rows() instead of insert_row()
        $self->insert_row($table, $columns, \@row);
    }
    close(App::Repository::DBI::FILE);

    &App::sub_exit() if ($App::trace);
}

#############################################################################
# export_rows()
#############################################################################

=head2 export_rows()

    * Signature: $rep->export_rows($table, $file);
    * Signature: $rep->export_rows($table, $file, $options);
    * Param:     $table        string
    * Param:     $file         string
    * Param:     $options      named
    * Param:     columns       ARRAY     names of columns of the fields in the file
    * Param:     replace       boolean   rows should replace existing rows based on unique indexes
    * Param:     field_sep     char      character which separates the fields in the file (can by "\t")
    * Param:     field_quote   char      character which optionally encloses the fields in the file (i.e. '"')
    * Param:     field_escape  char      character which escapes the quote chars within quotes (i.e. "\")
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $rep->export_rows("usr","usr.dat");

    # root:x:0:0:root:/root:/bin/bash
    $rep->export_rows("usr", "passwd.dat" ,{
        field_sep => ":",
        columns => [ "username", "password", "uid", "gid", "comment", "home_directory", "shell" ],
    });

=cut

sub export_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $file, $options) = @_;

    my $columns = $options->{columns} || $self->{table}{$table}{columns};
    my $rows = $self->get_rows($table, {}, $columns);
    my $field_sep = $options->{field_sep} || ",";
    my $field_quote = $options->{field_quote};
    my $field_escape = $options->{field_escape};

    open(App::Repository::DBI::FILE, "> $file") || die "Unable to open $file for writing: $!";
    my ($i, $value);
    foreach my $row (@$rows) {
        if ($field_quote) {
            for ($i = 0; $i <= $#$row; $i++) {
                print App::Repository::DBI::FILE $field_sep if ($i > 0);
                $value = $row->[$i];
                if ($value =~ /$field_sep/) {
                    if ($field_escape) {
                        $value =~ s/$field_escape/$field_escape$field_escape/g;
                        $value =~ s/$field_quote/$field_escape$field_quote/g;
                    }
                    print App::Repository::DBI::FILE $field_quote, $value, $field_quote;
                }
                else {
                    print App::Repository::DBI::FILE $value;
                }
            }
        }
        else {
            print App::Repository::DBI::FILE join($field_sep, @$row), "\n";
        }
    }
    close(App::Repository::DBI::FILE);

    &App::sub_exit() if ($App::trace);
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Locking (Concurrency Management)

=cut

# this is a write lock for the table
sub _lock_table {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;
    if (! $self->{locked}) {   # I have locked it myself, so I don't need to again
        my ($name, $dbname, $context, $rlock);
        $name = $self->{name};
        $dbname = $self->{dbname};
        $context = $self->{context};
        $rlock = $context->resource_locker($name);  # get the one that corresponds to this repository
        $rlock->lock("db.$dbname.$table");
        $self->{locked} = 1;
    }
    &App::sub_exit() if ($App::trace);
}

# unlocks the write lock for the table
sub _unlock_table {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;
    if ($self->{locked}) {
        my ($name, $dbname, $context, $rlock);
        $name = $self->{name};
        $dbname = $self->{dbname};
        $context = $self->{context};
        $rlock = $context->resource_locker($name);  # get the one that corresponds to this repository
        $rlock->unlock("db.$dbname.$table");
        delete $self->{locked};
    }
    &App::sub_exit() if ($App::trace);
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Miscellaneous

=cut

#############################################################################
# summarize()
#############################################################################

=head2 summarize()

    * Signature: $summarized_rows = $rep->summarize($rows, $columns, $summcolidx, $formulas);
    * Param:     $rows             [][]
    * Param:     $columns          []
    * Param:     $summcolidx       []
    * Param:     $formulas         {}
    * Return:    $summarized_rows  []
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    @rows = (
        [ 5, "Jim", "Green", 13.5, 320, ],
        [ 3, "Bob", "Green",  4.2, 230, ],
        [ 9, "Ken", "Green", 27.4, 170, ],
        [ 2, "Kim", "Blue",  11.7, 440, ],
        [ 7, "Jan", "Blue",  55.1,  90, ],
        [ 1, "Ben", "Blue",  22.6, 195, ],
    );
    @columns = ( "id", "name", "team", "rating", "score" );
    @summcolidx = ( 2 );  # "team"
    %formulas = (
        rating => "{sum(rating)}/{count(rating)}",
    );

    $summarized_rows = $rep->summarize(\@rows, \@columns, \@summcolidx, \%formulas);

=cut

sub summarize {
    &App::sub_entry if ($App::trace);
    my ($self, $rows, $columns, $summcolidx, $formulas) = @_;

    my (@summary_rows, $summary_row, $create_summary);
    my ($key, $nextkey, $row, $rowidx, $colidx, $numcols, $column, $elem);
    my (%total, $formula);

    $numcols = $#$columns + 1;

    for ($rowidx = 0; $rowidx <= $#$rows; $rowidx++) {
        $row = $rows->[$rowidx];

        $key = "Total";
        $key = join(",", @{$row->[$rowidx]}[@$summcolidx])
            if (defined $summcolidx);

        # accumulate totals
        for ($colidx = 0; $colidx < $numcols; $colidx++) {
            $column = $columns->[$colidx];
            $elem = $rows->[$rowidx][$colidx];
            if (defined $elem && $elem ne "") {
                if (defined $total{$column}) {
                    $total{"sum($column)"} += ($elem+0);
                }
                else {
                    $total{"sum($column)"} = ($elem+0);
                }
                $total{"$column"} = $elem;
            }
        }
        if (defined $total{"count(*)"}) {
            $total{"count(*)"} ++;
        }
        else {
            $total{"count(*)"} = 1;
        }

        # look ahead to see if we need to produce a summary row yet
        $create_summary = 0;
        if ($rowidx == $#$rows) {
            $create_summary = 1;
        }
        else {
            $nextkey = "Total";
            $nextkey = join(",", @{$rows->[$rowidx+1]}[@$summcolidx])
                if (defined $summcolidx);
            $create_summary = 1 if ($nextkey ne $key);
        }

        if ($create_summary) {

            $summary_row = [];

            for ($colidx = 0; $colidx < $numcols; $colidx++) {
                $column = $columns->[$colidx];
                $formula = $formulas->{$column};
    
                $elem = "";
                if (defined $formula) {   # match {
                    $formula =~ s/\{([^\}]+)\}/\$total{"$1"}/g;
                    $formula = "\$elem = $formula;";
                    eval $formula;
                    #$elem = "[$formula] $@" if ($@);
                }
                else {
                    $elem = $total{"sum($column)"};
                }
            }
    
            push (@summary_rows, $summary_row);
        }
    }
    &App::sub_exit(\@summary_rows) if ($App::trace);
    \@summary_rows;
}

#############################################################################
# sort()
#############################################################################

=head2 sort()

    * Signature: $sorted_rows = $rep->sort($rows, $sortkeys);
    * Signature: $sorted_rows = $rep->sort($rows, $sortkeys, $sorttype);
    * Signature: $sorted_rows = $rep->sort($rows, $sortkeys, $sorttype, $sortdir);
    * Param:     $rows             [][]
    * Param:     $sortkeys       []
    * Param:     $sorttype         []
    * Param:     $sortdir          []
    * Return:    $sorted_rows      []
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: (to sort arrayrefs)

      @rows = (
          [ 5, "Jim", "Green", 13.5, 320, ],
          [ 3, "Bob", "Green",  4.2, 230, ],
          [ 9, "Ken", "Green", 27.4, 170, ],
          [ 2, "Kim", "Blue",  11.7, 440, ],
          [ 7, "Jan", "Blue",  55.1,  90, ],
          [ 1, "Ben", "Blue",  22.6, 195, ],
      );
      # @columns = ( "id", "name", "team", "rating", "score" ); # not needed
      @sortkeys = ( 2, 4 );          # "team", "score" (descending)
      @sorttype = ( "C", "N" );      # Character, Numeric
      @sortdir = ( "asc", "desc" );  # Ascending, Descending
      $sorted_rows = $rep->sort(\@rows, \@sortkeys, \@sorttype, \@sortdir);

    OR (to sort hashrefs)

      @rows = (
          { id => 5, name => "Jim", team => "Green", rating => 13.5, score => 320, },
          { id => 3, name => "Bob", team => "Green", rating =>  4.2, score => 230, },
          { id => 9, name => "Ken", team => "Green", rating => 27.4, score => 170, },
          { id => 2, name => "Kim", team => "Blue",  rating => 11.7, score => 440, },
          { id => 7, name => "Jan", team => "Blue",  rating => 55.1, score =>  90, },
          { id => 1, name => "Ben", team => "Blue",  rating => 22.6, score => 195, },
      );
      # @columns = ( "id", "name", "team", "rating", "score" ); # not needed
      @sortkeys = ( "team", "score" );          # "team", "score" (descending)
      @sorttype = ( "C", "N" );      # Character, Numeric
      @sortdir = ( "asc", "desc" );  # Ascending, Descending
      $sorted_rows = $rep->sort(\@rows, \@sortkeys, \@sorttype, \@sortdir);

=cut

sub sort {
    &App::sub_entry if ($App::trace);
    my ($self, $rows, $sortkeys, $sorttype, $sortdir) = @_;

    @App::Repository::sort_keys  = @$sortkeys;
    @App::Repository::sort_types = ($sorttype ? @$sorttype : ());
    @App::Repository::sort_dirs  = ($sortdir ? @$sortdir : ());

    my ($sorted_rows);
    if ($rows && ref($rows) eq "ARRAY" && $#$rows > 0) {
        if (ref($rows->[0]) eq "ARRAY") {
            $sorted_rows = [ sort rows_by_indexed_values @$rows ];
        }
        else {
            $sorted_rows = [ sort hashes_by_indexed_values @$rows ];
        }
    }
    else {
        $sorted_rows = $rows;
    }
    &App::sub_exit($sorted_rows) if ($App::trace);
    return($sorted_rows);
}

#############################################################################
# evaluate_expressions()
#############################################################################

=head2 evaluate_expressions()

    * Signature: $nrows = $rep->evaluate_expressions($table, $params, $cols, $rows, $options);
    * Param:     $table     string
    * Param:     $params    HASH,scalar
    * Param:     $cols      ARRAY
    * Param:     $rows      ARRAY
    * Param:     $options   undef,HASH
    * Return:    $nrows     integer
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage:

    $rep->evaluate_expressions($table, $params, \@cols, $rows, \%options);

tbd.

=cut

sub evaluate_expressions {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $rows, $options) = @_;
    $options ||= {};
    my %options = %$options;
    my $column_defs = $self->{table}{$table}{column};
    my (@expr_col_idx, @expr_col, $col, %colidx);

    for (my $i = 0; $i <= $#$cols; $i++) {
        $col = $cols->[$i];
        $colidx{$col} = $i;
        if ($column_defs->{$col}{expr}) {
            push(@expr_col, $col);
            push(@expr_col_idx, $i);
        }
    }

    if ($#expr_col > -1) {
        foreach my $row (@$rows) {
            for (my $i = 0; $i <= $#expr_col; $i++) {
                $col = $expr_col[$i];
                $row->[$expr_col_idx[$i]] = $self->evaluate_expression($column_defs->{$col}{expr}, $row, \%colidx, $column_defs);
            }
        }
    }

    &App::sub_exit() if ($App::trace);
}

sub evaluate_expression {
    &App::sub_entry if ($App::trace);
    my ($self, $expr, $values, $validx, $column_defs) = @_;

    my $value = $expr;
    if ($values) {
        my ($col, $val, $idx);
        if (ref($values) eq "ARRAY") {
            while ($value =~ /\{([^{}]+)\}/) {
                $col = $1;
                $idx = $validx->{$col};
                if (defined $idx) {
                    $val = $values->[$idx];
                    $val = $column_defs->{$col}{expr} if (!defined $val);
                    $val = $column_defs->{$col}{default} if (!defined $val);
                    $val = "undef" if (!defined $val);
                    $val = "($val)" if ($val =~ /[-\+\*\/]/);
                }
                else {
                    $val = "undef";
                }
                $value =~ s/\{$col\}/$val/g || last;
            }
        }
        else {
            while ($value =~ /\{([^{}]+)\}/) {
                $col = $1;
                $val = $values->{$col};
                $val = App::Reference->get($col, $values) if (!defined $val && $col =~ /[\[\]\{\}\.]/);
                $val = $column_defs->{$col}{expr} if (!defined $val);
                $val = $column_defs->{$col}{default} if (!defined $val);
                $val = "undef" if (!defined $val);
                $val = "($val)" if ($val =~ /[-\+\*\/]/);
                $value =~ s/\{$col\}/$val/g || last;
            }
        }
    }
    $value = $self->evaluate_constant_expression($value);

    &App::sub_exit($value) if ($App::trace);
    return($value);
}

sub evaluate_constant_expression {
    &App::sub_entry if ($App::trace);
    my ($self, $value) = @_;
    my $NUM = "-?[0-9.]+";

    while ($value =~ /\([^()]*\)/) {
        $value =~ s/\(([^()]+)\)/$self->evaluate_constant_expression($1)/eg;
    }
    if ($value =~ m!^[-\+\*/0-9\.\s]+$!) {  # all numeric expression
        $value =~ s/\s+//g;
    }
    while ($value =~ s!($NUM)\s*([\*/])\s*($NUM)!($2 eq "*") ? ($1 * $3) : ($3 ? ($1 / $3) : "undef")!e) {
        # nothing else needed
    }
    while ($value =~ s!($NUM)\s*([\+-])\s*($NUM)!($2 eq "+") ? ($1 + $3) : ($1 - $3)!e) {
        # nothing else needed
    }
    $value = undef if ($value =~ /undef/);

    &App::sub_exit($value) if ($App::trace);
    return($value);
}

#############################################################################
# serial()
#############################################################################

=head2 serial()

    * Signature: $serial_num = $repository->serial($category);
    * Param:     $category          string
    * Return:    $serial_num        integer
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $serial_num = $repository->serial($category);

=cut

my %serial_number;
sub serial {
    &App::sub_entry if ($App::trace);
    my ($self, $category) = @_;
    my ($serial);
    if (!defined $serial_number{$category}) {
        $serial_number{$category} = 1;
        $serial = 1;
    }
    else {
        $serial = ++$serial_number{$category};
    }
    &App::sub_exit($serial) if ($App::trace);
    return($serial);
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Metadata

=cut

#############################################################################
# _load_rep_metadata()
#############################################################################

=head2 _load_rep_metadata()

    * Signature: $repository->_load_rep_metadata();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->_load_rep_metadata();

Initializes the repository metadata information from the config.

    * List of tables (+ displayable labels)
    * List of column types (+ displayable labels)

Then it calls _load_rep_metadata_from_source() in order for the repository
itself to be consulted for its metadata information.

=cut

sub _load_rep_metadata {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    my ($table, $tables, $table_defs, $table_def, $native_table, $idx, $label, @label);

    # load up all possible information from the native metadata
    $self->_load_rep_metadata_from_source();

    # start with the list of tables that was configured (or the empty list)
    $tables = $self->{tables};
    if (!defined $tables) {
        $tables = [];
        $self->{tables} = $tables;
    }

    # start with the hash of tables defined (or the empty hash)
    $table_defs = $self->{table};
    if (!defined $table_defs) {
        $table_defs = {};
        $self->{table} = $table_defs;
    }

    # for each table named in the configuration, give it a number up front
    for ($idx = 0; $idx <= $#$tables; $idx++) {
        $table = $tables->[$idx];
        $table_defs->{$table}{idx} = $idx;
    }

    # for each table in the hash (random order), add them to the end
    foreach $table (keys %$table_defs) {
        $table_def = $table_defs->{$table};
        $table_def->{name} = $table;
        if (! $table_def->{label}) {
            $label = $table;
            if ($self->{auto_label}) {
                $label = lc($label);
                $label =~ s/^([a-z])/uc($1)/e;
                $label =~ s/(_[a-z])/uc($1)/eg;
                $label =~ s/_+/ /g;
            }
            $table_def->{label} = $label;
        }
 
        # table has not been added to the list and it's not explicitly "hidden", so add it
        if (!defined $table_def->{idx} && ! $table_def->{hide}) {
            push(@$tables, $table);
            $table_def->{idx} = $#$tables;

            # we're not hiding physical tables and a native table was defined, so make an entry
            if (! $self->{hide_physical}) {
                $native_table = $table_def->{native_table};
                if (defined $native_table) {
                    $table_defs->{$native_table} = $table_defs->{$table};
                }
            }
        }

        $self->{table_labels}{$table} = $table_def->{label};
    }

    my ($type, $types, $type_defs);

    # start with the hash of types defined (or the empty hash)
    $type_defs = $self->{type};
    if (!defined $type_defs) {
        $type_defs = {};
        $self->{type} = $type_defs;
    }

    # define the standard list of Repository types
    $types = [ "string", "text", "integer", "float", "date", "time", "datetime", "binary" ];
    $self->{types} = $types;

    # define the standard list of Repository labels
    $self->{type_labels} = {
        "string"   => "Characters",
        "text"     => "Text",
        "integer"  => "Integer",
        "float"    => "Number",
        "date"     => "Date",
        "time"     => "Time",
        "datetime" => "Date and Time",
        "binary"   => "Binary Data",
    };

    # figure the index in the array of each type
    for ($idx = 0; $idx <= $#$types; $idx++) {
        $type = $types->[$idx];
        $self->{type}{$type}{idx} = $idx;
    }
    &App::sub_exit() if ($App::trace);
}

#############################################################################
# _load_rep_metadata_from_source()
#############################################################################

=head2 _load_rep_metadata_from_source()

    * Signature: $repository->_load_rep_metadata_from_source();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $repository->_load_rep_metadata_from_source();

Loads repository metadata from the repository itself
(to complement metadata in the configuration and perhaps
override it).

The default implementation does nothing.
It is intended to be overridden in the subclass
(if the repository has any sort of metadata).

=cut

sub _load_rep_metadata_from_source {
    my ($self) = @_;
}

#############################################################################
# _load_table_metadata()
#############################################################################

=head2 _load_table_metadata()

    * Signature: $self->_load_table_metadata();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->_load_table_metadata();

First it calls _load_table_metadata_from_source() in order for the repository
itself to be consulted for any metadata information for the about the
table.

Then it initializes
the repository metadata information for that table from the config
information.

    * List of columns (+ displayable labels, types)
    * List of column types (+ displayable labels)

Then it determines the set of required columns whenever selecting
data from the table and clears the cache of selected rows
for the table.

=cut

sub _load_table_metadata {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;

    # if it's already been loaded, don't do it again
    return if (defined $self->{table}{$table}{loaded});
    $self->{table}{$table}{loaded} = 1;   # mark it as having been loaded

    my ($table_def, $columns, $column, $column_def, $idx, $native_column);

    $table_def = $self->{table}{$table};
    return if (!defined $table_def);

    # load up all additional information from the native metadata
    $self->_load_table_metadata_from_source($table);

    $columns = $table_def->{columns};
    if (! defined $columns) {
        $columns = [];
        $table_def->{columns} = $columns;
    }

    # for each column named in the configuration, give it a number up front
    for ($idx = 0; $idx <= $#$columns; $idx++) {
        $column = $columns->[$idx];
        $table_def->{column}{$column}{idx} = $idx;
    }

    # for each column in the hash (random order), add them to the end
    my ($label);
    foreach $column (keys %{$table_def->{column}}) {
        $column_def = $table_def->{column}{$column};
        $column_def->{name} = $column;
        if (! $column_def->{label}) {
            $label = $column;
            if ($self->{auto_label}) {
                $label = lc($label);
                $label =~ s/^([a-z])/uc($1)/e;
                $label =~ s/(_[a-z])/uc($1)/eg;
                $label =~ s/_+/ /g;
            }
            $column_def->{label} = $label;
        }
 
        # column has not been added to the list and it's not explicitly "hidden", so add it
        if (!defined $column_def->{idx} && ! $column_def->{hide}) {
            push(@$columns, $column);
            $idx = $#$columns;
            $column_def->{idx} = $idx;
            $column_def->{alias}  = "c$idx" if (!defined $column_def->{alias});

            # we're not hiding physical columns and a native table was defined, so make an entry
            if (! $self->{hide_physical}) {
                $native_column = $column_def->{native_column};
                if (defined $native_column &&
                    $native_column ne $column &&
                    !defined $table_def->{column}{$native_column}) {
                    $table_def->{column}{$native_column} = $table_def->{column}{$column};
                }
            }
        }

        $table_def->{column_labels}{$column} = $column_def->{label};
    }

    ######################################################################
    # primary key
    ######################################################################

    # if a non-reference scalar, assume it's a comma-separated list and split it
    if ($table_def->{primary_key} && ! ref($table_def->{primary_key})) {
        $table_def->{primary_key} = [ split(/ *, */, $table_def->{primary_key}) ];
    }

    &App::sub_exit() if ($App::trace);
}

#############################################################################
# _load_table_metadata_from_source()
#############################################################################

=head2 _load_table_metadata_from_source()

    * Signature: $repository->_load_table_metadata_from_source();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->_load_table_metadata_from_source();

Loads metadata for an individual table from the repository itself
(to complement metadata in the configuration and perhaps
override it).

The default implementation does nothing.
It is intended to be overridden in the subclass
(if the repository has any sort of metadata).

=cut

sub _load_table_metadata_from_source {
    my ($self, $table) = @_;
}

#############################################################################
# METHODS
#############################################################################

=head1 Methods: Miscellaneous

=cut

#####################################################################
# _init()
#####################################################################

=head2 _init()

    * Signature: $repository->_init();
    * Param:     defer_connection     integer
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->_init();

Every Service constructor (Repository is derived from Service) will
invoke the _init() method near the end of object construction.

The standard behavior for repositories (implemented here) in _init() is
to initialize the "numrows" and "error" attributes,
call _init2(), connect to the repository,
and load the repository metadata.

=cut

sub _init {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    $self->{numrows} = 0;
    $self->{error}   = "";

    $self->_init2();

    if (!$self->{defer_connection} && !$self->_connect()) {
        print STDERR "Error on connect():";
        foreach (keys %$self) {
            print STDERR " $_=[", $self->{$_}, "]";
        }
        print STDERR "\n";
        return(undef);
    }

    $self->_load_rep_metadata();
    &App::sub_exit() if ($App::trace);
}

#############################################################################
# _init2()
#############################################################################

=head2 _init2()

    * Signature: $repository->_init2();
    * Param:     defer_connection    integer
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->_init2();

The default behavior of _init2() does nothing
and is intended to be overridden (if necessary) in the subclass which
implements the details of access to the physical data store.

=cut

sub _init2 {    # OVERRIDE IN SUBCLASS TO GET NON-DEFAULT CAPABILITIES
    my $self = shift;
}

#############################################################################
# service_type()
#############################################################################

=head2 service_type()

Returns 'Repository'.

    * Signature: $service_type = App::Repository->service_type();
    * Param:     void
    * Return:    $service_type  string
    * Since:     0.01

    $service_type = $widget->service_type();

=cut

sub service_type () { 'Repository'; }

#############################################################################
# current_datetime()
#############################################################################

=head2 current_datetime()

Returns 'Repository'.

    * Signature: $current_datetime = App::Repository->current_datetime();
    * Param:     void
    * Return:    $current_datetime  string
    * Since:     0.01

    $current_datetime = $widget->current_datetime();

=cut

sub current_datetime {
    return (time2str("%Y-%m-%d %H:%M:%S",time()));
}

#############################################################################
# rows_by_indexed_values()
#############################################################################

=head2 rows_by_indexed_values()

    * Signature: &App::Repository::rows_by_indexed_values($a,$b);
    * Param:     $a            []
    * Param:     $b            []
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    @data = (
        [ 5, "Jim", "Red",    13.5, ],
        [ 3, "Bob", "Green",   4.2, ],
        [ 9, "Ken", "Blue",   27.4, ],
        [ 2, "Kim", "Yellow", 11.7, ],
        [ 7, "Jan", "Purple", 55.1, ],
    );

    @App::Repository::sort_keys = ( 1, 3, 2 );
    @App::Repository::sort_types = ("C", "N", "C");
    @App::Repository::sort_dirs = ("asc", "desc", "desc");
    # OR @App::Repository::sort_dirs = ("_asc", "_desc", "_desc");
    # OR @App::Repository::sort_dirs = ("UP", "DOWN", "DOWN");

    @sorted_data = sort rows_by_indexed_values @data;

The rows_by_indexed_values() function is used to sort rows of data
based on indexes, data types, and directions.

=cut

sub rows_by_indexed_values {
    my ($pos, $idx, $type, $dir, $sign);
    for ($pos = 0; $pos <= $#App::Repository::sort_keys; $pos++) {
        $idx  = $App::Repository::sort_keys[$pos];
        $type = $App::Repository::sort_types[$pos];
        $dir  = $App::Repository::sort_dirs[$pos];
        if (defined $type && $type eq "N") {
            $sign = ($a->[$idx] <=> $b->[$idx]);
        }
        else {
            $sign = ($a->[$idx] cmp $b->[$idx]);
        }
        if ($sign) {
            $sign = -$sign if (defined $dir && $dir =~ /^_?[Dd]/); # ("DOWN", "desc", "_desc", etc.)
            return ($sign);
        }
    }
    return 0;
}

#############################################################################
# hashes_by_indexed_values()
#############################################################################

=head2 hashes_by_indexed_values()

    * Signature: &App::Repository::hashes_by_indexed_values($a,$b);
    * Param:     $a            []
    * Param:     $b            []
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    @data = (
        { size => 5, name => "Jim", color => "Red",    score => 13.5, ],
        { size => 3, name => "Bob", color => "Green",  score =>  4.2, ],
        { size => 9, name => "Ken", color => "Blue",   score => 27.4, ],
        { size => 2, name => "Kim", color => "Yellow", score => 11.7, ],
        { size => 7, name => "Jan", color => "Purple", score => 55.1, ],
    );

    @App::Repository::sort_keys = ( "size", "color", "name" );
    @App::Repository::sort_types = ("C", "N", "C");
    @App::Repository::sort_dirs = ("asc", "desc", "desc");
    # OR @App::Repository::sort_dirs = ("_asc", "_desc", "_desc");
    # OR @App::Repository::sort_dirs = ("UP", "DOWN", "DOWN");

    @sorted_data = sort hashes_by_indexed_values @data;

The hashes_by_indexed_values() function is used to sort rows of data
based on indexes, data types, and directions.

=cut

sub hashes_by_indexed_values {
    my ($pos, $key, $type, $dir, $sign);
    for ($pos = 0; $pos <= $#App::Repository::sort_keys; $pos++) {
        $key  = $App::Repository::sort_keys[$pos];
        $type = $App::Repository::sort_types[$pos];
        $dir  = $App::Repository::sort_dirs[$pos];
        if (defined $type && $type eq "N") {
            $sign = ($a->{$key} <=> $b->{$key});
        }
        else {
            $sign = ($a->{$key} cmp $b->{$key});
        }
        if ($sign) {
            $sign = -$sign if (defined $dir && $dir =~ /^_?[Dd]/); # ("DOWN", "desc", "_desc", etc.)
            return ($sign);
        }
    }
    return 0;
}

#############################################################################
# DESTROY()
#############################################################################

=head2 DESTROY()

    * Signature: $self->DESTROY();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.01

    Sample Usage: 

    $self->DESTROY();   # never called explicitly. called by Perl itself.

The DESTROY() method is called when the repository object is release from
memory.  This happen when the calling program lets the variable holding the
object reference go out of scope, sets the variable to something else,
or exits the program without otherwise releasing the object.

The DESTROY() method simply calls disconnect() to make sure that all
connection-related resources are freed.  This is safe, assuming (correctly)
that the disconnect() method may be called without negative consequences
even when already disconnected from the repository.

=cut

sub DESTROY {
    my $self = shift;
    $self->_disconnect();
}

=head1 ACKNOWLEDGEMENTS

 * Author:  Stephen Adkins <stephen.adkins@officevision.com>
 * License: This is free software. It is licensed under the same terms as Perl itself.

=head1 SEE ALSO

L<C<App::Context>|App::Context>,
L<C<App::Service>|App::Service>

=cut

1;

