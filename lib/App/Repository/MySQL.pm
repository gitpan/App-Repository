
######################################################################
## File: $Id: MySQL.pm,v 1.8 2004/09/02 21:02:08 spadkins Exp $
######################################################################

use App::Repository::DBI;

package App::Repository::MySQL;
$VERSION = do { my @r=(q$Revision: 1.8 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r};

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

sub _dsn {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    my $dbidriver  = "mysql";
    my $dbname     = $self->{dbname};
    my $dbuser     = $self->{dbuser};
    my $dbpass     = $self->{dbpass};
    my $dbschema   = $self->{dbschema};
    my $dbhost     = $self->{dbhost};
    my $dbport     = $self->{dbport};

    die "ERROR: missing DBI driver and/or db name [$dbidriver,$dbname] in configuration.\n"
        if (!$dbidriver || !$dbname);

    # NOTE: mysql_client_found_rows=true is important for the following condition.
    # If an update is executed against a row that exists, but its values do not change,
    # MySQL does not ordinarily report this as a row that has been affected by the
    # statement.  However, we occasionally need to know if the update found the row.
    # We really don't care if the values were changed or not.  To get this behavior,
    # we need to set this option.

    my $dsn = "dbi:${dbidriver}:database=${dbname}";
    $dsn .= ";host=$dbhost" if ($dbhost);
    $dsn .= ";port=$dbport" if ($dbport);  # if $dbhost not supplied, $dbport is path to Unix socket
    $dsn .= ";mysql_client_found_rows=true";

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

1;

