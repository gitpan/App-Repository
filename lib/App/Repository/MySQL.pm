
######################################################################
## File: $Id: MySQL.pm 3522 2005-11-21 20:24:57Z spadkins $
######################################################################

use App::Repository::DBI;

package App::Repository::MySQL;
$VERSION = do { my @r=(q$Revision: 3522 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r};

@ISA = ( "App::Repository::DBI" );

use strict;

=head1 NAME

App::Repository::MySQL - a MySQL database, accessed through the Repository interface

=head1 SYNOPSIS

   use App::Repository::MySQL;

   (see man pages for App::Repository and App::Repository::DBI for additional methods)

   ...

=cut

=head1 DESCRIPTION

The App::Repository::MySQL class encapsulates all access to a MySQL database.

=cut

sub _connect {
    &App::sub_entry if ($App::trace);
    my $self = shift;

    if (!defined $self->{dbh}) {
        my $dsn = $self->_dsn();
        my $attr = $self->_attr();

        while (1) {
            eval {
                $self->{dbh} = DBI->connect($dsn, $self->{dbuser}, $self->{dbpass}, $attr);
                $self->{dbh}{mysql_auto_reconnect} = 1;
            };
            if ($@) {
                delete $self->{dbh};
                if ($@ =~ /Lost connection/ || $@ =~ /server has gone away/) {
                    $self->{context}->log("DBI Exception (retrying) in _connect(): $@");
                    sleep(1);
                }
                else {
                    $self->{context}->log("DBI Exception (fail) in _connect(): $@");
                    die $@;
                }
            }
            else {
                last;
            }
        }
        die "Can't connect to database" if (!$self->{dbh});
    }

    &App::sub_exit(defined $self->{dbh}) if ($App::trace);
    return(defined $self->{dbh});
}
sub _dsn {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    my $dbdriver   = "mysql";
    $self->{dbdriver} = $dbdriver if (!$self->{dbdriver});

    my $dsn = $self->{dbdsn};
    if (!$dsn) {
        my $dbname     = $self->{dbname};
        my $dbuser     = $self->{dbuser};
        my $dbpass     = $self->{dbpass};
        my $dbschema   = $self->{dbschema};
        my $dbhost     = $self->{dbhost};
        my $dbport     = $self->{dbport};

        die "ERROR: missing DBI driver and/or db name [$dbdriver,$dbname] in configuration.\n"
            if (!$dbdriver || !$dbname);

        # NOTE: mysql_client_found_rows=true is important for the following condition.
        # If an update is executed against a row that exists, but its values do not change,
        # MySQL does not ordinarily report this as a row that has been affected by the
        # statement.  However, we occasionally need to know if the update found the row.
        # We really don't care if the values were changed or not.  To get this behavior,
        # we need to set this option.

        $dsn = "dbi:${dbdriver}:database=${dbname}";
        $dsn .= ";host=$dbhost" if ($dbhost);
        $dsn .= ";port=$dbport" if ($dbport);  # if $dbhost not supplied, $dbport is path to Unix socket
        $dsn .= ";mysql_client_found_rows=true";
    }

    &App::sub_exit($dsn) if ($App::trace);
    return($dsn);
}

sub _mk_select_sql_suffix {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $options) = @_;
    my $suffix = "";
    $options = {} if (!$options);
    if ($options->{endrow}) {
        $suffix = "limit $options->{endrow}\n";
    }
    &App::sub_exit($suffix) if ($App::trace);
    return($suffix);
}

sub _last_inserted_id {
    my ($self) = @_;
    return($self->{dbh}{mysql_insertid});
}

sub _load_table_key_metadata {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;

    return if (! $table);
    my $table_def = $self->{table}{$table};
    return if (! $table_def);
    my $dbh = $self->{dbh};

    # if not defined at all, try to get it from the database
    my (@primary_key, @alternate_key, @index, @key, $key_name, $non_unique);
    if ($table_def->{phys_table} && (! defined $table_def->{primary_key} || ! defined $table_def->{alternate_key})) {
        local $dbh->{FetchHashKeyName} = 'NAME_lc';
        my $sth = $dbh->prepare("SHOW INDEX FROM $table");
        my $hashes = $dbh->selectall_arrayref($sth, { Columns=>{} });
        foreach my $hash (@$hashes) {
             if ($key_name && $hash->{key_name} ne $key_name) {
                 if ($key_name eq 'PRIMARY') {
                     @primary_key = @key;
                 }                          
                 elsif ($non_unique) {
                     push(@index, [@key]);
                 }                          
                 else {
                     push(@alternate_key, [@key]);
                 }                          
                 @key = ();
             }
             $non_unique = $hash->{non_unique};
             $key_name = $hash->{key_name};
             push(@key, $hash->{column_name});
         }
         if ($key_name) {
             if ($key_name eq 'PRIMARY') {
                 @primary_key = @key;
             }                          
             elsif ($non_unique) {
                 push(@index, [@key]);
             }                          
             else {
                 push(@alternate_key, [@key]);
             }                          
         }
        
         $table_def->{primary_key} = \@primary_key if (!$table_def->{primary_key});
         $table_def->{alternate_key} = \@alternate_key if (!$table_def->{alternate_key} && $#alternate_key > -1);
    }
    &App::sub_exit() if ($App::trace);
}

# The following patch purportedly adds primary_key() detection directly
# to the DBD where it belongs.  Until this is in, I may want to
# duplicate the code in this module.
#diff -ru DBD-mysql-2.9003/lib/DBD/mysql.pm new/lib/DBD/mysql.pm
#--- DBD-mysql-2.9003/lib/DBD/mysql.pm  Mon Oct 27 14:26:08 2003
#+++ new/lib/DBD/mysql.pm   Tue Mar 2 08:03:17 2004
#@@ -282,7 +282,22 @@
#    return map { $_ =~ s/.*\.//; $_ } $dbh->tables();
#}
#-
#+sub primary_key {
#+    my ($dbh, $catalog, $schema, $table) = @_;
#+    my $table_id = $dbh->quote_identifier($catalog, $schema, $table);
#+    local $dbh->{FetchHashKeyName} = 'NAME_lc';
#+    my $desc_sth = $dbh->prepare("SHOW INDEX FROM $table_id");
#+    my $desc = $dbh->selectall_arrayref($desc_sth, { Columns=>{} });
#+    my %keys;
#+    foreach my $row (@$desc) {
#+        if ($row->{key_name} eq 'PRIMARY') {
#+            $keys{$row->{column_name}} = $row->{seq_in_index};
#+        }                          
#+     }
#+     my (@keys) = sort { $keys{$a} <=> $keys{$b} } keys %keys;
#+     return (@keys);
#+}
#+      
#sub column_info {
#    my ($dbh, $catalog, $schema, $table, $column) = @_;
#    return $dbh->set_err(1, "column_info doesn't support table wildcard")

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
    * Param:     method        string    [basic=invokes generic superclass to do work]
    * Param:     local         boolean   file is on client machine rather than database server
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

#SYNTAX:
#LOAD DATA [LOW_PRIORITY | CONCURRENT] [LOCAL] INFILE 'file_name.txt'
#    [REPLACE | IGNORE]
#    INTO TABLE tbl_name
#    [FIELDS
#        [TERMINATED BY 'string']
#        [[OPTIONALLY] ENCLOSED BY 'char']
#        [ESCAPED BY 'char' ]
#    ]
#    [LINES
#        [STARTING BY 'string']
#        [TERMINATED BY 'string']
#    ]
#    [IGNORE number LINES]
#    [(col_name_or_user_var,...)]
#    [SET col_name = expr,...)]

sub import_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $file, $options) = @_;

    if ($options->{method} && $options->{method} eq "basic") {
        $self->SUPER::import_rows($table, $file, $options);
    }
    else {
        my $local = $options->{local};
        $local = 1 if (!defined $local);
        my $local_modifier = $local ? " local" : "";
        my $sql = "load data$local_modifier infile '$file' into table $table";
        if ($options->{field_sep} || $options->{field_quote} || $options->{field_escape}) {
            $sql .= "\nfields";
            $sql .= "\n   terminated by '$options->{field_sep}'" if ($options->{field_sep});
            $sql .= "\n   optionally enclosed by '$options->{field_quote}'" if ($options->{field_quote});
            $sql .= "\n   escaped by '$options->{field_escape}'" if ($options->{field_escape});
        }
        if ($options->{columns}) {
            $sql .= "\n(" . join(",", @{$options->{columns}}) . ")";
        }
        my $debug_sql = $self->{context}{options}{debug_sql};
        if ($debug_sql) {
            print "DEBUG_SQL: import_rows()\n";
            print $sql;
        }
        my $retval = $self->{dbh}->do($sql);
        if ($debug_sql) {
            print "DEBUG_SQL: import_rows() = [$retval]\n";
        }
    }

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
    * Param:     method        string    [basic=invokes generic superclass to do work]
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

#SELECT ... INTO OUTFILE is the complement of LOAD DATA INFILE; the syntax for the
#export_options part of the statement consists of the same FIELDS and LINES clauses
#that are used with the LOAD DATA INFILE statement.
#See Section 13.2.5, .LOAD DATA INFILE Syntax..

#SELECT
#    [ALL | DISTINCT | DISTINCTROW ]
#      [HIGH_PRIORITY]
#      [STRAIGHT_JOIN]
#      [SQL_SMALL_RESULT] [SQL_BIG_RESULT] [SQL_BUFFER_RESULT]
#      [SQL_CACHE | SQL_NO_CACHE] [SQL_CALC_FOUND_ROWS]
#    select_expr, ...
#    [INTO OUTFILE 'file_name' export_options
#      | INTO DUMPFILE 'file_name']
#    [FROM table_references
#      [WHERE where_definition]
#      [GROUP BY {col_name | expr | position}
#        [ASC | DESC], ... [WITH ROLLUP]]
#      [HAVING where_definition]
#      [ORDER BY {col_name | expr | position}
#        [ASC | DESC] , ...]
#      [LIMIT {[offset,] row_count | row_count OFFSET offset}]
#      [PROCEDURE procedure_name(argument_list)]
#      [FOR UPDATE | LOCK IN SHARE MODE]]

sub export_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $file, $options) = @_;

    if ($options->{method} && $options->{method} eq "basic") {
        $self->SUPER::export_rows($table, $file, $options);
    }
    else {
        my $columns = $options->{columns} || $self->{table}{$table}{columns};
        my $sql = "select\n   " . join(",\n   ", @$columns) . "\ninto outfile '$file'";
        if ($options->{field_sep} || $options->{field_quote} || $options->{field_escape}) {
            $sql .= "\nfields";
            $sql .= "\n   terminated by '$options->{field_sep}'" if ($options->{field_sep});
            $sql .= "\n   optionally enclosed by '$options->{field_quote}'" if ($options->{field_quote});
            $sql .= "\n   escaped by '$options->{field_escape}'" if ($options->{field_escape});
        }
        my $debug_sql = $self->{context}{options}{debug_sql};
        if ($debug_sql) {
            print "DEBUG_SQL: export_rows()\n";
            print $sql;
        }
        my $retval = $self->{dbh}->do($sql);
        if ($debug_sql) {
            print "DEBUG_SQL: export_rows() = [$retval]\n";
        }
    }
    
    &App::sub_exit() if ($App::trace);
}

1;

