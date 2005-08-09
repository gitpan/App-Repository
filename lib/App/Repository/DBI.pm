
######################################################################
## File: $Id: DBI.pm,v 1.27 2005/08/09 18:52:25 spadkins Exp $
######################################################################

use App;
use App::Repository;

package App::Repository::DBI;
$VERSION = do { my @r=(q$Revision: 1.27 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r};

@ISA = ( "App::Repository" );

use Data::Dumper;

use strict;

=head1 NAME

App::Repository::DBI - a repository which relies on a DBI interface to a relational database (no caching)

=head1 SYNOPSIS

   use App::Repository::DBI;

   (see man page for App::Repository for additional methods)

   $rep = App::Repository::DBI->new();        # looks for %ENV, then config file
   $rep = App::Repository::DBI->new("mysql","mydb","user001","pass001");
   $rep = App::Repository::DBI->new("mysql","mydb","user001","pass001","port=3307");
   $rep = App::Repository::DBI->new("mysql","mydb","user001","pass001","port=3307","user001");

   $ok = $rep->_connect();         # initialize repository (will happen automatically in constructor)
   $ok = $rep->_disconnect();      # cleanup repository (will happen automatically in destructor)
   $rep->_is_connected();          # returns 1 if connected (ready for use), 0 if not
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
# $self->{dbdriver}   # standard DBI driver name ("mysql", "Oracle", etc.)
# $self->{dbname}     # the name of the database
# $self->{dbuser}     # database user name
# $self->{dbpass}     # database password
# $self->{dbschema}   # name of the schema within the database
# $self->{dbioptions} # additional dbi options to tack onto the dsn
# $self->{dbh}        # open DBI database handle

######################################################################
# INHERITED ATTRIBUTES
######################################################################

# BASIC
# $self->{name}       # name of this repository (often "db")
# $self->{conf}       # hash of config file data

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

The App::Repository::DBI class encapsulates all access to the database,
changing SQL statements into get(), save(), and delete() methods.

=cut

#############################################################################
# PUBLIC METHODS
#############################################################################

=head1 Public Methods

=cut

#############################################################################
# _connect()
#############################################################################

=head2 _connect()

    * Signature: $repository->_connect();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.50

    Sample Usage: 

    $repository->_connect();

Connects to the repository.  Most repositories have some connection
initialization that takes time and therefore should be done once.
Then many operations may be executed against the repository.
Finally the connection to the repository is closed (_disconnect()).

The default implementation of _connect() does nothing.
It is intended to be overridden in the subclass (if necessary).

=cut

sub _connect {
    &App::sub_entry if ($App::trace);
    my $self = shift;

    if (!defined $self->{dbh}) {
        my $dsn = $self->_dsn();
        my $attr = $self->_attr();
        $self->{dbh} = DBI->connect($dsn, $self->{dbuser}, $self->{dbpass}, $attr);
        die "Can't connect to database" if (!$self->{dbh});
    }

    &App::sub_exit(defined $self->{dbh}) if ($App::trace);
    return(defined $self->{dbh});
}

# likely overridden at the subclass level
sub _dsn {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;
    my ($dbdriver, $dbname, $dbuser, $dbpass, $dbschema);

    my $dsn = $self->{dbdsn};
    if (!$dsn) {
        my $dbdriver   = $self->{dbdriver} || $self->{dbdriver};
        my $dbname     = $self->{dbname};
        my $dbuser     = $self->{dbuser};
        my $dbpass     = $self->{dbpass};
        my $dbschema   = $self->{dbschema};

        die "ERROR: missing DBI driver and/or db name [$dbdriver,$dbname] in configuration.\n"
            if (!$dbdriver || !$dbname);

        $dsn = "dbi:${dbdriver}:database=${dbname}";
    }

    &App::sub_exit($dsn) if ($App::trace);
    return($dsn);
}

# likely overridden at the subclass level
sub _attr {
    &App::sub_entry if ($App::trace);
    my $attr = {
        PrintError         => 0,
        AutoCommit         => 1,
        RaiseError         => 1,
        #ShowErrorStatement => 1,  # this doesn't seem to include the right SQL statement. very confusing.
    };
    &App::sub_exit($attr) if ($App::trace);
    return($attr);
}

sub _dbh {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    &App::sub_exit($self->{dbh}) if ($App::trace);
    return($self->{dbh});
}

#############################################################################
# _disconnect()
#############################################################################

=head2 _disconnect()

    * Signature: $repository->_disconnect();
    * Param:     void
    * Return:    void
    * Throws:    App::Exception::Repository
    * Since:     0.50

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

sub _disconnect {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    if (defined $self->{dbh} && !($self->{preconnected})) {
        my $dbh = $self->{dbh};
        $dbh->disconnect;
        delete $self->{dbh};
    }
    &App::sub_exit(1) if ($App::trace);
    1;
}

sub _disconnect_client_only {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    if ($self->{dbh}) {
        $self->{dbh}{InactiveDestroy} = 1;
        delete $self->{dbh};
    }
    &App::sub_exit(1) if ($App::trace);
    1;
}

sub _is_connected {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my $retval = ((defined $self->{dbh}) ? 1 : 0);
    &App::sub_exit($retval) if ($App::trace);
    return ($retval);
}

#############################################################################
# PRIVATE METHODS
#############################################################################

=head1 Private Methods

=cut

######################################################################
# INITIALIZATION
######################################################################

sub _init2 {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my ($name);

    $name = $self->{name};
    if (defined $self->{dbh}) {
        $self->{preconnected} = 1;
    }
    else {
        my $options = $self->{context}{options} || {};

        my $config_from_options = 1;
        foreach my $var qw(dbdsn dbdriver dbhost dbport dbname dbuser dbpass dbschema dbioptions) {
            if ($self->{$var}) {
                $config_from_options = 0;
                last;
            }
        }

        if ($config_from_options) {
            foreach my $var qw(dbdsn dbdriver dbhost dbport dbname dbuser dbpass dbschema dbioptions) {
                if (defined $options->{$var}) {
                    $self->{$var} = $options->{$var};
                }
            }
        }
    }
    &App::sub_exit() if ($App::trace);
}

sub _get_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;

    # we only need the first row
    $options = {} if (!$options);
    if (! $options->{endrow}) {
        $options->{endrow} = $options->{startrow} || 1;
    }

    my ($sql, $dbh, $row);
    if ($self->{table}{$table}{rawaccess}) {
        $sql = $self->_mk_select_sql($table, $params, $cols, $options);
    }
    else {
        $sql = $self->_mk_select_joined_sql($table, $params, $cols, $options);
    }
    $self->{sql} = $sql;

    $dbh = $self->{dbh};
    if (!$dbh) {
        $self->_connect();
        $dbh = $self->{dbh};
    }

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _get_row()\n";
        print $sql;
    }
    while (1) {
        eval {
            $row = $dbh->selectrow_arrayref($sql);
        };
        if ($@) {
            $row = undef;
            if ($@ =~ /Lost connection/ || $@ =~ /server has gone away/) {
                $self->{context}->log("DBI Exception (retrying) in _get_row(): $@");
                $self->_disconnect();
                sleep(1);
                $self->_connect();
                $dbh = $self->{dbh};
            }
            else {
                $self->{context}->log("DBI Exception (fail) in _get_row(): $@");
                die $@;
            }
        }
        else {
            last;
        }
    }
    if ($debug_sql) {
        print "DEBUG_SQL: nrows [", (defined $row ? 1 : 0), "] $DBI::errstr\n";
        if ($debug_sql >= 2) {
            print "DEBUG_SQL: [", ($row ? join("|",map { defined $_ ? $_ : "undef" } @$row) : ""), "]\n";
        }
        print "\n";
    }

    &App::sub_exit($row) if ($App::trace);
    return($row);
}

sub _get_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;

    my ($sql, $rows, $startrow, $endrow);
    if ($self->{table}{$table}{rawaccess}) {
        $sql = $self->_mk_select_sql($table, $params, $cols, $options);
    }
    else {
        $sql = $self->_mk_select_joined_sql($table, $params, $cols, $options);
    }
    $self->{sql} = $sql;
    die "empty SQL query for table [$table] (does table exist?)" if (!$sql);

    $self->_connect() if (!$self->{dbh});

    $options  = {} if (!$options);
    $startrow = $options->{startrow} || 0;
    $endrow   = $options->{endrow} || 0;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _get_rows()\n";
        print $sql;
    }
    while (1) {
        eval {
            $rows = $self->_selectrange_arrayref($sql, $startrow, $endrow);
        };
        if ($@) {
            $rows = [];
            if ($@ =~ /Lost connection/ || $@ =~ /server has gone away/) {
                $self->{context}->log("DBI Exception (retrying) in _get_rows(): $@");
                $self->_disconnect();
                sleep(1);
                $self->_connect();
            }
            else {
                $self->{context}->log("DBI Exception (fail) in _get_rows(): $@");
                die $@;
            }
        }
        else {
            last;
        }
    }
    if ($debug_sql) {
        print "DEBUG_SQL: nrows [", (defined $rows ? ($#$rows+1) : 0), "] $DBI::errstr\n";
        if ($debug_sql >= 2) {
            foreach my $row (@$rows) {
                print "DEBUG_SQL: [", join("|",map { defined $_ ? $_ : "undef"} @$row), "]\n";
            }
        }
        print "\n";
    }

    &App::sub_exit($rows) if ($App::trace);
    return($rows);
}

# modified from the DBD::_::db::selectall_arrayref in DBI.pm
sub _selectrange_arrayref {
    &App::sub_entry if ($App::trace);
    my ($self, $stmt, $startrow, $endrow, $attr, @bind) = @_;
    my $dbh = $self->{dbh};
    return [] if (!$dbh);

    my $sth = (ref $stmt) ? $stmt : $dbh->prepare($stmt, $attr);
    if ($sth) {
        $sth->execute(@bind) || return;
        my $slice = $attr->{Slice}; # typically undef, else hash or array ref
        if (!$slice and $slice=$attr->{Columns}) {
            if (ref $slice eq 'ARRAY') { # map col idx to perl array idx
                $slice = [ @{$attr->{Columns}} ];       # take a copy
                for (@$slice) { $_-- }
            }
        }
        my $retval = $self->_fetchrange_arrayref($sth, $startrow, $endrow, $slice);
        &App::sub_exit($retval) if ($App::trace);
        return($retval);
    }
    else {
        &App::sub_exit() if ($App::trace);
        return();
    }
}

# modified from the DBD::_::st::fetchall_arrayref in DBI.pm
sub _fetchrange_arrayref {
    &App::sub_entry if ($App::trace);
    my ($self, $sth, $startrow, $endrow, $slice) = @_;
    $slice = [] if (! defined $slice);
    $startrow = 0 if (!defined $startrow);
    $endrow = 0 if (!defined $endrow);
    my $mode = ref $slice;
    my @rows;
    my $row;
    my ($rownum);
    if ($mode eq 'ARRAY') {
        # we copy the array here because fetch (currently) always
        # returns the same array ref. XXX
        if (@$slice) {
            $rownum = 0;
            while ($row = $sth->fetch) {
                $rownum++;
                last if ($endrow > 0 && $rownum > $endrow);
                push @rows, [ @{$row}[ @$slice] ] if ($rownum >= $startrow);
            }
            $sth->finish if ($endrow > 0 && $rownum > $endrow);
        }
        else {
            # return $sth->_fetchall_arrayref;
            $rownum = 0;
            while ($row = $sth->fetch) {
                $rownum++;
                last if ($endrow > 0 && $rownum > $endrow);
                push @rows, [ @$row ] if ($rownum >= $startrow);
            }
            $sth->finish if ($endrow > 0 && $rownum > $endrow);
        }
    }
    elsif ($mode eq 'HASH') {
        if (keys %$slice) {
            my @o_keys = keys %$slice;
            my @i_keys = map { lc } keys %$slice;
            $rownum = 0;
            while ($row = $sth->fetchrow_hashref('NAME_lc')) {
                my %hash;
                @hash{@o_keys} = @{$row}{@i_keys};
                $rownum++;
                last if ($endrow > 0 && $rownum > $endrow);
                push @rows, \%hash if ($rownum >= $startrow);
            }
            $sth->finish if ($endrow > 0 && $rownum > $endrow);
        }
        else {
            # XXX assumes new ref each fetchhash
            while ($row = $sth->fetchrow_hashref()) {
                $rownum++;
                last if ($endrow > 0 && $rownum > $endrow);
                push @rows, $row if ($rownum >= $startrow);
            }
            $sth->finish if ($endrow > 0 && $rownum > $endrow);
        }
    }
    else { Carp::croak("fetchall_arrayref($mode) invalid") }
    &App::sub_exit(\@rows) if ($App::trace);
    return \@rows;
}

######################################################################
# SQL CREATE METHODS (new methods not defined in App::Repository)
######################################################################

sub _mk_where_clause {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $options) = @_;
    my ($where, $column, $param, $value, $colnum, $repop, $sqlop, $column_def, $quoted);
    my ($tabledef, $tabcols, %sqlop, $alias, $dbexpr);

    $tabledef = $self->{table}{$table};
    $alias    = $tabledef->{alias};
    $tabcols  = $tabledef->{column};
    %sqlop = (
        'contains' => 'like',
        'matches'  => 'like',
        'eq'       => '=',
        'ne'       => '!=',
        'le'       => '<=',
        'lt'       => '<',
        'ge'       => '>=',
        'gt'       => '>',
        'in'       => 'in',
    );

    $where = "";
    $params = {} if (!$params);
    my $param_order = $params->{"_order"};
    if (!defined $param_order && ref($params) eq "HASH") {
        $param_order = [ (keys %$params) ];
    }
    if (defined $param_order && $#$param_order > -1) {
        my ($include_null, $inferred_op, @where);
        for ($colnum = 0; $colnum <= $#$param_order; $colnum++) {
            $param = $param_order->[$colnum];
            $column = $param;
            $sqlop = "=";
            $repop = "";
            $inferred_op = 1;
            # check if $column contains an embedded operation, i.e. "name.eq", "name.contains"
            if ($param =~ /^(.*)\.([^.]+)$/) {
                $repop = $2;
                $inferred_op = 0;
                if ($sqlop{$repop}) {
                    $column = $1;
                    $sqlop = $sqlop{$repop};
                }
            }

            if ($repop eq "verbatim") {
                push(@where, "$params->{$param}");
                next;
            }

            $column_def = $tabcols->{$column};

            if (!defined $column_def) {
                if ($param =~ /^begin_(.*)/) {
                    $column = $1;
                    $sqlop = ">=";
                }
                elsif ($param =~ /^end_(.*)/) {
                    $column = $1;
                    $sqlop = "<=";
                }
                $column_def = $tabcols->{$column};
            }

            next if (!defined $column_def);  # skip if the column is unknown

            if (! defined $params->{$param}) {
                # $value = "?";   # TODO: make this work with the "contains/matches" operators
                if (!$sqlop || $sqlop eq "=") {
                    push(@where, "$column is null");
                }
                else {
                    push(@where, "$column is not null");
                }
            }
            else {
                $value = $params->{$param};

                next if ($inferred_op && $value eq "ALL");

                if (ref($value) eq "ARRAY") {
                    $value = join(",", @$value);
                }

                if ($value =~ s/^@\[(.*)\]$/$1/) {  # new @[] expressions replace !expr!
                    $quoted = 0;
                }
                elsif ($value =~ s/^@\{(.*)\}$/$1/) {  # replaced !expr!, but @{x} is interp'd by perl so deprecate!
                    $quoted = 0;
                }
                elsif ($value =~ s/^!expr!//) { # deprecated (ugh!)
                    $quoted = 0;
                }
                elsif ($value =~ /,/ && ! $tabledef->{param}{$param}{no_auto_in_param}) {
                    $quoted = (defined $column_def->{quoted}) ? ($column_def->{quoted}) : ($value !~ /^-?[0-9.,]+$/);
                }
                else {
                    $quoted = (defined $column_def->{quoted}) ? ($column_def->{quoted}) : ($value !~ /^-?[0-9.]+$/);
                }

                next if ($inferred_op && !$quoted && $value eq "");

                $include_null = 0;

                if ($repop eq "contains") {
                    $value =~ s/'/\\'/g;
                    $value = "'%$value%'";
                }
                elsif ($repop eq "matches") {
                    $value =~ s/_/\\_/g;
                    $value =~ s/'/\\'/g;
                    $value =~ s/\*/%/g;
                    $value =~ s/\?/_/g;
                    $value = "'$value'";
                }
                elsif ($sqlop eq "in" || $sqlop eq "=") {
                    if (! defined $value || $value eq "NULL") {
                        $sqlop = "is";
                        $value = "null";
                    }
                    else {
                        if ($value =~ s/NULL,//g || $value =~ s/,NULL//) {
                            $include_null = 1;
                        }
                        if ($quoted) {
                            $value =~ s/'/\\'/g;
                            if ($value =~ /,/ && ! $tabledef->{param}{$param}{no_auto_in_param}) {
                                $value =~ s/,/','/g;
                                $value = "('$value')";
                                $sqlop = "in";
                            }
                            else {
                                $value = "'$value'";
                                $sqlop = "=";
                            }
                        }
                        else {
                            if ($value =~ /,/ && ! $tabledef->{param}{$param}{no_auto_in_param}) {
                                $value = "($value)";
                                $sqlop = "in";
                            }
                            else {
                                $sqlop = "=";
                            }
                        }
                    }
                }
                elsif ($quoted) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
                $dbexpr = $column_def->{dbexpr};
                if ($dbexpr && $dbexpr ne "$alias.$column") {
                    $column = $dbexpr;
                    $column =~ s/$alias.//g;
                }
                if ($include_null) {
                    push(@where, "($column $sqlop $value or $column is null)");
                }
                else {
                    push(@where, "$column $sqlop $value");
                }
            }
        }
        if ($#where > -1) {
            $where = "where " . join("\n  and ", @where) . "\n";
        }
    }
    &App::sub_exit($where) if ($App::trace);
    $where;
}

sub _mk_select_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});

    $params = $self->_key_to_params($table,$params) if (!$params || !ref($params));  # $params is undef/scalar => $key
    $cols = [$cols] if (!ref($cols));
    $options = {} if (!$options);

    my ($sql, $order_by, $direction, $col, $colnum, $dir);
    $order_by = $options->{order_by} || $options->{ordercols} || [];  # {ordercols} is deprecated
    $order_by = [$order_by] if (!ref($order_by));
    $direction = $options->{direction} || $options->{directions};     # {directions} is deprecated
    my $modifier = $options->{distinct} ? " distinct" : "";

    $sql = "select$modifier\n   " . join(",\n   ", @$cols) . "\nfrom $table\n";
    $sql .= $self->_mk_where_clause($table, $params);

    if (defined $order_by && $#$order_by > -1) {
        for ($colnum = 0; $colnum <= $#$order_by; $colnum++) {
            $col = $order_by->[$colnum];
            if ($col =~ /^(.+)\.asc$/) {
                $col = $1;
                $dir = " asc";
            }
            elsif ($col =~ /^(.+)\.desc$/) {
                $col = $1;
                $dir = " desc";
            }
            else {
                $dir = "";
                if ($direction && ref($direction) eq "HASH" && defined $direction->{$col}) {
                    if ($direction->{$col} =~ /^asc$/i) {
                        $dir = " asc";
                    }
                    elsif ($direction->{$col} =~ /^desc$/i) {
                        $dir = " desc";
                    }
                }
            }
            $sql .= ($colnum == 0) ? "order by\n   $col$dir" : ",\n   $col$dir";
        }
        $sql .= "\n";
    }
    my $suffix = $self->_mk_select_sql_suffix($table, $options);
    $sql .= $suffix if ($suffix);
    
    &App::sub_exit($sql) if ($App::trace);
    return($sql);
}

sub _mk_select_joined_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});

    if (!defined $params || $params eq "") {
        $params = {};
    }
    elsif (!ref($params)) {
        $params = $self->_key_to_params($table,$params);  # $params is undef/scalar => $key
    }
    $cols = [$cols] if (!ref($cols));
    $options = {} if (!$options);

    my ($order_by, $direction, $param_order, $col, $dir);
    $order_by = $options->{order_by} || $options->{ordercols} || [];  # {ordercols} is deprecated
    $order_by = [$order_by] if (!ref($order_by));
    $direction = $options->{direction} || $options->{directions};     # {directions} is deprecated
    my $modifier = $options->{distinct} ? " distinct" : "";

    $param_order = $params->{"_order"};
    if (!defined $param_order && ref($params) eq "HASH") {
        $param_order = [ (keys %$params) ];
    }

    my ($startrow, $endrow, $auto_extend, $keycolidx, $writeref, $reptyperef, $summarykeys);
    $startrow    = $options->{startrow}    || 0;
    $endrow      = $options->{endrow}      || 0;
    $auto_extend = $options->{auto_extend} || 0;
    $keycolidx   = $options->{keycolidx};
    $writeref    = $options->{writeref};
    $reptyperef  = $options->{reptyperef};
    $summarykeys = $options->{summarykeys};

    my ($table_def, $tablealiases, $tablealiashref);

    $table_def = $self->{table}{$table};
    return undef if (!$table_def);
    $self->_load_table_metadata($table) if (!defined $table_def->{loaded});

    $tablealiases   = $table_def->{tablealiases};
    $tablealiashref = $table_def->{tablealias};

    ############################################################
    # Record indexes of all requested columns
    ############################################################
    my ($idx, $column, %columnidx, @write, @reptype);

    for ($idx = 0; $idx <= $#$cols; $idx++) {
        $column = $cols->[$idx];
        if (! defined $columnidx{$column}) {
            $columnidx{$column} = $idx;
        }
        $write[$idx] = 1;            # assume every field writable
        $reptype[$idx] = "string";   # assume every field a string (most general type)
    }

    ############################################################
    # ensure that the primary key and sort keys are included
    ############################################################
    my ($dbexpr, $columnalias, $columntype, $colidx, $column_def, $quoted);
    my (%dbexpr, @select_phrase, $group_reqd, @group_dbexpr, %reqd_tables);
    my (@keycolidx, $primary_key, $primary_table);
    my ($is_summary, %is_summary_key, $summaryexpr, @group_summarykeys);

    $is_summary = (defined $summarykeys && $#$summarykeys >= 0);

    $primary_table = "";
    if ($is_summary) {
        foreach $column (@$summarykeys) {         # primary key is list of summary keys
            $is_summary_key{$column} = 1;
            $colidx = $columnidx{$column};
            if (! defined $colidx && $auto_extend) {
                push(@$cols, $column);            # add the column to the list
                $colidx = $#$cols;
                $columnidx{$column} = $colidx;
            }
            if (defined $colidx) {
                push(@keycolidx, $colidx);
                $write[$colidx] = 0;              # keys aren't editable
            }
        }
    }
    else {  # non-summary (detail) table rows
        $primary_key = $table_def->{primary_key}; # primary key is in the metadata
        if ($primary_key) {
            $primary_key = [$primary_key] if (!ref($primary_key));
            foreach $column (@$primary_key) {
                $colidx = $columnidx{$column};
                if (! defined $colidx && $auto_extend) {
                    push(@$cols, $column);         # add the column to the list
                    $colidx = $#$cols;
                    $columnidx{$column} = $colidx;
                }
                if (defined $colidx) {
                    push(@keycolidx, $colidx);
                    $write[$colidx] = 0;        # keys aren't editable
                }
    
                $dbexpr = $table_def->{column}{$column}{dbexpr}; # take note of table the key is on
                if ($dbexpr && $dbexpr =~ /^([a-zA-Z][a-zA-Z0-9_]*)\.[a-zA-Z_][a-zA-Z_0-9]*$/) {
                    $primary_table = $1;
                }
            }
        }
    }

    if ($auto_extend) {
        if (defined $order_by && ref($order_by) eq "ARRAY") {
            foreach $column (@$order_by) {          # foreach sort key
                if ($column && ! defined $columnidx{$column} && $auto_extend) {
                    push(@$cols, $column);     # add the column to the list
                    $columnidx{$column} = $#$cols;
                }
            }
        }
    }

    for ($idx = 0; $idx <= $#$cols; $idx++) {
        $column = $cols->[$idx];
        $column_def = $table_def->{column}{$column};
        if (!defined $column_def) {
            push(@select_phrase, "NULL u$idx");
            next;
        }

        $columnalias   = $column_def->{alias};
        $dbexpr        = $column_def->{dbexpr};
        $reptype[$idx] = $column_def->{type};

        # if the field is not defined, or it is not a simple field on the primary table...
        if (!defined $dbexpr || $dbexpr !~ /^$primary_table\.[a-zA-Z_][a-zA-Z0-9_]*$/) {
            $write[$idx] = 0;    # consider it *not* writable
        }

        ############################################################
        # accumulate select expressions and their aliases
        ############################################################
        if ($is_summary) {
            if ($is_summary_key{$column}) {
                if (defined $dbexpr) {
                    push(@select_phrase, "$dbexpr $columnalias");
                    push(@group_summarykeys, $columnalias);
                }
            }
            else {
                $summaryexpr = $column_def->{summary};
                if (!defined $summaryexpr || $summaryexpr eq "") {
                    $columntype = $column_def->{type};
                    if ($columntype eq "integer" || $columntype eq "number") {
                        $summaryexpr = "avg(\$)";
                    }
                    else {
                        $summaryexpr = "count(distinct(\$))";
                    }
                }
                if (defined $dbexpr) {
                    $summaryexpr =~ s#\$#$dbexpr#g;   # substitute the dbexpr into the summaryexpr
                }
                else {
                    $summaryexpr = "NULL";
                }
                push(@select_phrase, "$summaryexpr $columnalias") if ($summaryexpr);
            }
        }
        else {
            push(@select_phrase, (defined $dbexpr) ? "$dbexpr $columnalias" : "NULL $columnalias");
        }

        ############################################################
        # get the expression from the config info
        ############################################################

        if (!defined $dbexpr || $dbexpr eq "") {
            $dbexpr{$column} = "NULL";
        }
        else {
            ############################################################
            # save selected columns for reference
            ############################################################
            $dbexpr{$column} = $dbexpr;

            ############################################################
            # accumulate group-by columns and whether grouping reqd
            ############################################################
            if (($dbexpr =~ /sum *\(/i) ||
                ($dbexpr =~ /min *\(/i) ||
                ($dbexpr =~ /max *\(/i) ||
                ($dbexpr =~ /avg *\(/i) ||
                ($dbexpr =~ /std *\(/i) ||
                ($dbexpr =~ /stddev *\(/i) || # Oracle extension (supported by MySQL)
                ($dbexpr =~ /count *\(/i)) {
                $group_reqd = 1;
            }
            else {
                push(@group_dbexpr, $columnalias);
            }

            ############################################################
            # For each table, mentioned in the select expression...
            ############################################################
            $self->_require_tables($dbexpr, \%reqd_tables, $tablealiashref, 1);
        }
    }

    ############################################################
    # copy data out if a reference is given
    ############################################################
    if (defined $keycolidx && ref($keycolidx) eq "ARRAY") {
        @$keycolidx = @keycolidx;
    }
    if (defined $writeref && ref($writeref) eq "ARRAY") {
        @$writeref = @write;
    }
    if (defined $reptyperef && ref($reptyperef) eq "ARRAY") {
        @$reptyperef = @reptype;
    }

    ############################################################
    # create order-by columns
    ############################################################
    my (@order_by_dbexpr, $order_by_dbexpr);
    if (defined $order_by && ref($order_by) eq "ARRAY") {
        my ($dir);

        for ($idx = 0; $idx <= $#$order_by; $idx++) {
            $column = $order_by->[$idx];
            $dir = "";
            if ($column =~ /^(.+)\.asc$/) {
                $column = $1;
                $dir = " asc";
            }
            elsif ($column =~ /^(.+)\.desc$/) {
                $column = $1;
                $dir = " desc";
            }
            $column_def = $table_def->{column}{$column};
            next if (!defined $column_def);

            $order_by_dbexpr = $dbexpr{$column};
            if (!$order_by_dbexpr) {
                $order_by_dbexpr = $column_def->{dbexpr};
                $dbexpr{$column} = $order_by_dbexpr;
                $self->_require_tables($order_by_dbexpr, \%reqd_tables, $tablealiashref, 1);
            }

            $columnalias = $column_def->{alias};
            if (defined $columnidx{$column} && $columnalias) {
                $order_by_dbexpr = $columnalias;
            }

            if ($order_by_dbexpr) {
                if ($dir) {
                    $order_by_dbexpr .= $dir;
                }
                else {
                    if ($direction && ref($direction) eq "HASH" && defined $direction->{$column}) {
                        if ($direction->{$column} =~ /^asc$/i) {
                            $order_by_dbexpr .= " asc";
                        }
                        elsif ($direction->{$column} =~ /^desc$/i) {
                            $order_by_dbexpr .= " desc";
                        }
                    }
                }
                push(@order_by_dbexpr, $order_by_dbexpr);
            }
        }
    }

    ############################################################
    # create initial where conditions for the selected rows
    ############################################################

    #print $self->{context}->dump(), "\n";

    my %sqlop = (
        'contains' => 'like',
        'matches'  => 'like',
        'eq'       => '=',
        'ne'       => '!=',
        'le'       => '<=',
        'lt'       => '<',
        'ge'       => '>=',
        'gt'       => '>',
        'in'       => 'in',
    );

    my ($where_condition, @join_conditions, @criteria_conditions, $param, $repop, $sqlop, $paramvalue);
    my ($include_null, $inferred_op);
    for ($idx = 0; $idx <= $#$param_order; $idx++) {

        $param = $param_order->[$idx];
        next if (!defined $param || $param eq "");

        $column = $param;

        #if ($param eq "_key") {
        #    # o TODO: enable multi-field primary keys (this assumes one-field only)
        #    # o TODO: enable non-integer primary key fields (this assumes integer, no quotes)
        #    $column = $table_def->{primary_key};  # assumes one column primary key
        #    $dbexpr = $table_def->{column}{$column}{dbexpr};
        #    if ($paramvalue =~ /,/) {
        #        $where_condition = "$dbexpr in ($paramvalue)";  # assumes one column, non-quoted primary key
        #    }
        #    else {
        #        $where_condition = "$dbexpr = $paramvalue";     # assumes one column, non-quoted primary key
        #    }
        #    push(@criteria_conditions, $where_condition);
        #    next;
        #}

        $sqlop = "=";
        $repop = "";
        $inferred_op = 1;
        # check if $param contains an embedded operation, i.e. "name.eq", "name.contains"
        if ($param =~ /^(.*)\.([^.]+)$/) {
            $repop = $2;
            $inferred_op = 0;
            if ($sqlop{$repop}) {
                $column = $1;
                $sqlop = $sqlop{$repop};
            }
        }

        if ($repop eq "verbatim") {
            push(@criteria_conditions, $params->{$param});
            next;
        }

        $column_def = $table_def->{column}{$column};

        if (!defined $column_def) {
            if ($param =~ /^begin_(.*)/) {
                $column = $1;
                $sqlop = ">=";
            }
            elsif ($param =~ /^end_(.*)/) {
                $column = $1;
                $sqlop = "<=";
            }
            $column_def = $table_def->{column}{$column};
        }

        next if (!defined $column_def);  # skip if the column is unknown

        $include_null = 0;

        if (! defined $params->{$param}) {
            # $paramvalue = "?";   # TODO: make this work with the "contains/matches" operators
            $sqlop = (!$sqlop || $sqlop eq "=") ? "is" : "is not";
            $paramvalue = "null";
        }
        else {
            $paramvalue = $params->{$param};

            next if (defined $table_def->{param}{$param}{all_value} &&
                     $paramvalue eq $table_def->{param}{$param}{all_value});

            next if ($inferred_op && $paramvalue eq "ALL");

            if (ref($paramvalue) eq "ARRAY") {
                $paramvalue = join(",", @$paramvalue);
            }

            if ($paramvalue =~ s/^@\[(.*)\]$/$1/) {  # new @[] expressions replace !expr!
                $quoted = 0;
            }
            elsif ($paramvalue =~ s/^@\{(.*)\}$/$1/) {  # new @{} don't work.. perl interpolates... deprecate.
                $quoted = 0;
            }
            elsif ($paramvalue =~ s/^!expr!//) { # deprecated (ugh!)
                $quoted = 0;
            }
            elsif ($paramvalue =~ /,/ && ! $table_def->{param}{$param}{no_auto_in_param}) {
                $quoted = (defined $column_def->{quoted}) ? ($column_def->{quoted}) : ($paramvalue !~ /^-?[0-9.,]+$/);
            }
            else {
                $quoted = (defined $column_def->{quoted}) ? ($column_def->{quoted}) : ($paramvalue !~ /^-?[0-9.]+$/);
            }

            next if ($inferred_op && !$quoted && $paramvalue eq "");

            if ($repop eq "contains") {
                $paramvalue =~ s/'/\\'/g;
                $paramvalue = "'%$paramvalue%'";
            }
            elsif ($repop eq "matches") {
                $paramvalue =~ s/_/\\_/g;
                $paramvalue =~ s/'/\\'/g;
                $paramvalue =~ s/\*/%/g;
                $paramvalue =~ s/\?/_/g;
                $paramvalue = "'$paramvalue'";
            }
            elsif ($sqlop eq "in" || $sqlop eq "=") {

                if (! defined $paramvalue || $paramvalue eq "NULL") {
                    $sqlop = "is";
                    $paramvalue = "null";
                }
                else {
                    if ($paramvalue =~ s/NULL,//g || $paramvalue =~ s/,NULL//) {
                        $include_null = 1;
                    }
                    if ($quoted) {
                        $paramvalue =~ s/'/\\'/g;
                        if ($paramvalue =~ /,/ && ! $table_def->{param}{$param}{no_auto_in_param}) {
                            $paramvalue =~ s/,/','/g;
                            $paramvalue = "('$paramvalue')";
                            $sqlop = "in";
                        }
                        else {
                            $paramvalue = "'$paramvalue'";
                            $sqlop = "=";
                        }
                    }
                    else {
                        if ($paramvalue =~ /,/ && ! $table_def->{param}{$param}{no_auto_in_param}) {
                            $paramvalue = "($paramvalue)";
                            $sqlop = "in";
                        }
                        else {
                            $sqlop = "=";
                        }
                    }
                }
            }
            elsif ($quoted) {
                $paramvalue =~ s/'/\\'/g;
                $paramvalue = "'$paramvalue'";
            }
        }

        $dbexpr = $column_def->{dbexpr};
        if (defined $dbexpr && $dbexpr ne "") {
            $self->_require_tables($dbexpr, \%reqd_tables, $tablealiashref, 2);
            if ($include_null) {
                push(@criteria_conditions, "($dbexpr $sqlop $paramvalue or $dbexpr is null)");
            }
            else {
                push(@criteria_conditions, "$dbexpr $sqlop $paramvalue");
            }
        }
    }

#    THIS IS DEAD CODE.
#    I NEED TO FIGURE OUT WHAT IT USED TO DO SO I CAN FIGURE WHETHER I NEED
#    TO REWRITE IT AND REINSTATE IT IN THE CURRENT CODE BASE.
#    {
#        my ($paramsql_alias_table, %param_used, @params_to_be_used);
#        my ($cond1, $cond2, $expr, $p1, $p2, $p1val, $p2val, @pval);
#        my ($crit_lines);
#
#        $paramsql_alias_table = $self->{table}{aliases}{$table}{parametersql};
#        $paramsql_alias_table = $table if (!$dep_alias_table);
#        $crit_lines = $self->{table}{criterialines}{$dep_alias_table}
#
#        CRIT: foreach $expr (@crit_lines) {
#            @params_to_be_used = ();
#            if ($expr =~ /^ *([^ ].*[^ ]) *\? *([^ ].*[^ ]) *$/) {
#                $cond1 = $1;
#                $expr = $2;
#
#                if ($cond1 =~ /^#(.+)/) {
#                    $p = $1;
#                    @pval = $query->param($p);
#                    next if ($#pval <= 0);
#                }
#                elsif ($cond1 =~ /^([a-zA-Z0-9]+) *== *\*([a-zA-Z0-9]+) *$/) {
#                    $p1 = $1;
#                    $p2 = $2;
#                    next CRIT if (defined $param_used{$p1} || defined $param_used{$p2});
#                    $p1val = $query->param($p1);
#                    $p2val = $query->param($p2);
#                    next CRIT if (!defined $p1val || !defined $p2val || $p1val ne $p2val);
#                    push(@params_to_be_used, $p2);
#                }
#            }
#
#            $cond2 = $expr;
#            while ($cond2 =~ s/{([a-zA-Z0-9]+)}//) {
#                $p = $1;
#                @pval = $query->param($p);
#                next CRIT if (!defined @pval || $#pval < 0 || $pval[0] eq "");
#                next CRIT if (defined $param_used{$p});
#                push(@params_to_be_used, $p);
#                if ($expr =~ /'{$p}'/) {
#                    $p1val = "'" . join("','",@pval) . "'";
#                    $expr =~ s/'{$p}'/$p1val/;
#                }
#                else {
#                    $p1val = join(",",@pval);
#                    $expr =~ s/{$p}/$p1val/;
#                }
#            }
#            foreach (@params_to_be_used) {
#                $param_used{$_} = 1;
#            }
#            push(@criteria_conditions, $expr);
#            $self->_require_tables($expr, \%reqd_tables, $table_aliases, 2);
#        }
#    }

    ############################################################
    # put tables in table list in the standard order
    # and build the join criteria
    ############################################################

    my ($dbtable, $tablealias, @from_tables, $tableref);
    my (@outer_join_clauses);

    foreach $tablealias (@$tablealiases) {
        #print "checking table $tablealias\n";
        if ($reqd_tables{$tablealias}) {
            $dbtable = $tablealiashref->{$tablealias}{table};
            $tableref = ($dbtable) ? "$dbtable $tablealias" : $tablealias;
            $where_condition = $tablealiashref->{$tablealias}{joincriteria};
            if ($tablealiashref->{$tablealias}{cardinality_zero}) {
                push(@outer_join_clauses, "left join $tableref on $where_condition") if ($where_condition);
                #print "   $tablealias is [$dbtable] as [$tableref] where [$where_condition] (outer)\n";
            }
            else {
                push(@join_conditions, split(/ +and +/,$where_condition)) if ($where_condition);
                push(@from_tables, $tableref);
                #print "   $tablealias is [$dbtable] as [$tableref] where [$where_condition]\n";
            }
        }
    }
    if ($#from_tables == -1 && $#$tablealiases > -1) {
        $tablealias = $tablealiases->[0];
        $table = $tablealiashref->{$tablealias}{table};
        $tableref = ($table) ? "$table $tablealias" : $tablealias;
        push(@from_tables, $tableref);
    }

    ############################################################
    # create the SQL statement
    ############################################################

    my ($sql, $conjunction);

    if ($#select_phrase >= 0) {
        $sql = "select$modifier\n   " .
                        join(",\n   ",@select_phrase) . "\n" .
                 "from\n   " .
                        join(",\n   ",@from_tables) . "\n";
    }

    if ($#outer_join_clauses >= 0) {
        $sql .= join("\n",@outer_join_clauses) . "\n";
    }

    if ($#join_conditions >= 0) {
        $sql .= "where " . join("\n  and ",@join_conditions) . "\n";
    }
    $conjunction = "AND";
    $conjunction = $params->{"_conjunction"} if (defined $params);
    $conjunction = "AND" if (!defined $conjunction);
    $conjunction = uc($conjunction);
    if ($#criteria_conditions >= 0) {
        $sql .= ($#join_conditions == -1 ? "where " : "  and ");
        if ($conjunction eq "NOT_AND") {
            $sql .= "not (" . join("\n  and ",@criteria_conditions) . ")\n";
        }
        elsif ($conjunction eq "NOT_OR") {
            $sql .= "not (" . join("\n  or ",@criteria_conditions) . ")\n";
        }
        elsif ($conjunction eq "OR") {
            $sql .= "(" . join("\n  or ",@criteria_conditions) . ")\n";
        }
        else {
            $sql .= join("\n  and ",@criteria_conditions) . "\n";
        }
    }
    if ($#group_summarykeys >= 0) {
        $sql .= "group by\n   " . join(",\n   ",@group_summarykeys) . "\n";
    }
    elsif ($group_reqd && $#group_dbexpr >= 0) {
        $sql .= "group by\n   " . join(",\n   ",@group_dbexpr) . "\n";
    }
    if ($#order_by_dbexpr >= 0) {
        $sql .= "order by\n   " . join(",\n   ",@order_by_dbexpr) . "\n";
    }

    my $suffix = $self->_mk_select_sql_suffix($table, $options);
    $sql .= $suffix if ($suffix);

    ############################################################
    # return the SQL statement
    ############################################################
    &App::sub_exit($sql) if ($App::trace);
    return($sql);
}

sub _mk_select_sql_suffix {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $options) = @_;
    &App::sub_exit("") if ($App::trace);
    return("");
}

sub _require_tables {
    &App::sub_entry if ($App::trace >= 3);
    my ($self, $dbexpr, $reqd_tables, $relationship_defs, $require_type) = @_;
    #print "_require_tables($dbexpr,...,...,$require_type)\n";
    my ($relationship, $relationship2, @relationship, %tableseen, $dependencies);
    while ($dbexpr =~ s/([a-zA-Z_][a-zA-Z_0-9]*)\.[a-zA-Z_][a-zA-Z_0-9]*//) {
        if (defined $relationship_defs->{$1} && !$tableseen{$1}) {
            push(@relationship, $1);
            $tableseen{$1} = 1;
        }
        while ($relationship = pop(@relationship)) {
            if (! defined $reqd_tables->{$relationship}) {
                $reqd_tables->{$relationship} = $require_type;
                #print "table required: $relationship => $require_type\n";
                $dependencies = $relationship_defs->{$relationship}{dependencies};
                push(@relationship, @$dependencies)
                   if (defined $dependencies && ref($dependencies) eq "ARRAY");
            }
            elsif ($reqd_tables->{$relationship} < $require_type) {
                $reqd_tables->{$relationship} = $require_type;
                #print "table required: $relationship => $require_type\n";
            }
        }
    }
    &App::sub_exit() if ($App::trace >= 3);
}

# $insert_sql = $rep->_mk_insert_row_sql ($table, \@cols, \@row);
sub _mk_insert_row_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});
    my ($sql, $values, $col, $value, $colnum, $quoted);

    #print "_mk_insert_row_sql($table,\n   [",
    #    join(",",@$cols), "],\n   [",
    #    join(",",@$row), "])\n";

    if ($#$cols == -1) {
        $self->{error} = "Database->_mk_insert_row_sql(): no columns specified";
        return();
    }
    my $tabcols = $self->{table}{$table}{column};

    $sql = "insert into $table\n";
    $values = "values\n";
    for ($colnum = 0; $colnum <= $#$cols; $colnum++) {
        $col = $cols->[$colnum];
        if (!defined $row || $#$row == -1) {
            $value = "?";
        }
        else {
            $value = $row->[$colnum];
            if (!defined $value) {
                $value = "NULL";
            }
            else {
                $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                if ($quoted) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
            }
        }
        $sql .= ($colnum == 0) ? "  ($col" : ",\n   $col";
        if ($tabcols->{$col}{dbexpr_update}) {
            $value = sprintf($tabcols->{$col}{dbexpr_update}, $value);
        }
        $values .= ($colnum == 0) ? "  ($value" : ",\n   $value";
    }
    $sql .= ")\n";
    $values .= ")\n";
    $sql .= $values;
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

# $insert_sql = $rep->_mk_insert_sql ($table, \@cols, \@row, \%options);
sub _mk_insert_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $options) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});

    if (!ref($cols)) {
        $cols = [ $cols ];
        $row  = [ $row ];
    }
    elsif ($#$cols == -1) {
        die "Database->_mk_insert_sql(): no columns specified";
    }

    my ($col, $value, $colidx, $quoted);
    my $tabcols = $self->{table}{$table}{column};
    my $by_expression = ($options && $options->{by_expression}) ? 1 : 0;
    my @values = ();

    for ($colidx = 0; $colidx <= $#$cols; $colidx++) {
        $col = $cols->[$colidx];
        if (!defined $row || $#$row == -1) {
            push(@values, "?");
        }
        else {
            $value = $row->[$colidx];
            if (!defined $value) {
                push(@values, "NULL");
            }
            elsif ($by_expression) {
                push(@values, $value);
            }
            else {
                $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                if ($quoted) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
                if ($tabcols->{$col}{dbexpr_update}) {
                    $value = sprintf($tabcols->{$col}{dbexpr_update}, $value);
                }
                push(@values, $value);
            }
        }
    }
    my $sql = "insert into $table\n  (" . join(",\n   ",@$cols) . ")\nvalues\n  (" . join(@values) . ")\n";
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

# $update_sql = $rep->_mk_update_sql($table, \%params,    \@cols, \@row, \%options);
# $update_sql = $rep->_mk_update_sql($table, \@keycolidx, \@cols, \@row, \%options);
# $update_sql = $rep->_mk_update_sql($table, \@paramcols, \@cols, \@row, \%options);
# $update_sql = $rep->_mk_update_sql($table, $key,        \@cols, \@row, \%options);
# $update_sql = $rep->_mk_update_sql($table, undef,       \@cols, \@row, \%options);
sub _mk_update_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    die "Database->_mk_update_sql(): no columns specified" if (!$cols || $#$cols == -1);

    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});

    my $tabcols = $self->{table}{$table}{column};
    my $by_expression = $options->{by_expression};

    my (@noupdate, %noupdate, @set, $where);

    $noupdate[$#$cols] = 0;   # pre-extend the array
    %noupdate = ();
    $where = "";

    my ($colidx, $col, $value, $quoted);
    if (!defined $params) {
        $params = $self->{table}{$table}{primary_key};
        die "_mk_update_sql() can't update with undef params because {table}{$table}{primary_key} not defined"
            if (!defined $params);
        $params = [ $params ] if (!ref($params));
    }

    if (!ref($params)) {  # update by key!
        $value = $params;
        $col = $self->{table}{$table}{primary_key};
        die "_mk_update_sql() can't update with key because {table}{$table}{primary_key} not defined"
            if (!defined $col);
        if (!ref($col)) {
            $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
            if ($quoted && !$by_expression) {
                $value =~ s/'/\\'/g;
                $value = "'$value'";
            }
            $where = "where $col = $value\n";
            $noupdate{$col} = 1;
        }
        else {
            $params = $col;   # it wasn't a column, but an array of them
            my @where = ();
            my @values = $self->_key_to_values($value);
            for ($colidx = 0; $colidx <= $#$params; $colidx++) {
                $col = $params->[$colidx];
                $value = $values[$colidx];
                $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                if ($quoted && !$by_expression) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
                push(@where, "$col = $value");
                $noupdate{$col} = 1;
            }
            $where = "where " . join("\n  and ",@where) . "\n";
        }
    }
    elsif (ref($params) eq "HASH") {
        $where = $self->_mk_where_clause($table, $params);
    }
    elsif (ref($params) eq "ARRAY") {
        die "_mk_update_sql() can't update with no indexes/columns in params" if ($#$params == -1);
        my @where = ();
        if ($params->[0] =~ /^[0-9]+$/) {  # an array of indexes
            my $keycolidx = $params;  # @$params represents a set of array indices
            for (my $i = 0; $i <= $#$keycolidx; $i++) {
                $colidx = $keycolidx->[$i];
                $col = $cols->[$colidx];
                if (!defined $row || $#$row == -1) {
                    $value = "?";
                }
                else {
                    $value = $row->[$colidx];
                    if (!defined $value) {
                        $value = "NULL";
                    }
                    else {
                        $quoted = (defined $tabcols->{$col}{quoted})?($tabcols->{$col}{quoted}):($value !~ /^-?[0-9.]+$/);
                        if ($quoted) {
                            $value =~ s/'/\\'/g;
                            $value = "'$value'";
                        }
                    }
                }
                push(@where, "$col = $value");
                $noupdate[$colidx] = 1;
            }
        }
        else {   # an array of column names
        }
        $where = "where " . join("\n  and ",@where) . "\n" if ($#where > -1);
    }
    else {
        die "_mk_update_sql() unrecognized params type";
    }

    # Now determine what to "set"
    my $ref_row = ref($row);
    if (!$ref_row) {
        for ($colidx = 0; $colidx <= $#$cols; $colidx++) {
            next if ($noupdate[$colidx]);
            $col = $cols->[$colidx];
            next if ($noupdate{$col});
            push(@set, "$col = ?");
        }
    }
    else {
        my $is_array = ($ref_row eq "ARRAY");
        for ($colidx = 0; $colidx <= $#$cols; $colidx++) {
            next if ($noupdate[$colidx]);
            $col = $cols->[$colidx];
            next if ($noupdate{$col});
            $value = $is_array ? $row->[$colidx] : $row->{$col};
            if (!defined $value) {
                push(@set, "$col = NULL");
            }
            else {
                $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                if ($quoted && !$by_expression) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
                if ($tabcols->{$col}{dbexpr_update}) {
                    $value = sprintf($tabcols->{$col}{dbexpr_update}, $value);
                }
                push(@set, "$col = $value");
            }
        }
    }

    my $sql = "update $table set\n   " . join(",\n   ",@set) . "\n" . $where;
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

# $delete_sql = $rep->_mk_delete_sql($table, \%params,                   \%options);
# $delete_sql = $rep->_mk_delete_sql($table, \%params,    undef,  undef, \%options);
# $delete_sql = $rep->_mk_delete_sql($table, \@keycolidx, \@cols, \@row, \%options);
# $delete_sql = $rep->_mk_delete_sql($table, \@paramcols, \@cols, \@row, \%options);
# $delete_sql = $rep->_mk_delete_sql($table, $key,                       \%options);
# $delete_sql = $rep->_mk_delete_sql($table, $key,        undef,  undef, \%options);
# $delete_sql = $rep->_mk_delete_sql($table, undef,       \@cols, \@row, \%options);
sub _mk_delete_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;

    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});

    my $tabcols = $self->{table}{$table}{column};
    my $by_expression = $options->{by_expression};

    my $where = "";

    my ($colidx, $col, $value, $quoted);
    if (!defined $params) {
        if ($cols && $row) {
            $params = $self->{table}{$table}{primary_key};
            die "_mk_delete_sql() can't delete with undef params because {table}{$table}{primary_key} not defined"
                if (!defined $params);
            $params = [ $params ] if (!ref($params));
        }
        else {
            $params = {};
        }
    }

    if (!ref($params)) {  # delete by key!
        $value = $params;
        $col = $self->{table}{$table}{primary_key};
        die "_mk_delete_sql() can't delete with key because {table}{$table}{primary_key} not defined"
            if (!defined $col);
        if (!ref($col)) {
            $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
            if ($quoted && !$by_expression) {
                $value =~ s/'/\\'/g;
                $value = "'$value'";
            }
            $where = "where $col = $value\n";
        }
        else {
            $params = $col;   # it wasn't a column, but an array of them
            my @where = ();
            my @values = $self->_key_to_values($value);
            for ($colidx = 0; $colidx <= $#$params; $colidx++) {
                $col = $params->[$colidx];
                $value = $values[$colidx];
                $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                if ($quoted && !$by_expression) {
                    $value =~ s/'/\\'/g;
                    $value = "'$value'";
                }
                push(@where, "$col = $value");
            }
            $where = "where " . join("\n  and ",@where) . "\n";
        }
    }
    elsif (ref($params) eq "HASH") {
        $where = $self->_mk_where_clause($table, $params);
    }
    elsif (ref($params) eq "ARRAY") {
        die "_mk_delete_sql() can't delete with no indexes/columns in params" if ($#$params == -1);
        my @where = ();
        if ($params->[0] =~ /^[0-9]+$/) {  # an array of indexes
            my $keycolidx = $params;  # @$params represents a set of array indices
            for (my $i = 0; $i <= $#$keycolidx; $i++) {
                $colidx = $keycolidx->[$i];
                $col = $cols->[$colidx];
                if (!defined $row || $#$row == -1) {
                    $value = "?";
                }
                else {
                    $value = $row->[$colidx];
                    if (!defined $value) {
                        $value = "NULL";
                    }
                    else {
                        $quoted = (defined $tabcols->{$col}{quoted})?($tabcols->{$col}{quoted}):($value !~ /^-?[0-9.]+$/);
                        if ($quoted) {
                            $value =~ s/'/\\'/g;
                            $value = "'$value'";
                        }
                    }
                }
                push(@where, "$col = $value");
            }
        }
        else {   # an array of column names
        }
        $where = "where " . join("\n  and ",@where) . "\n" if ($#where > -1);
    }
    else {
        die "_mk_delete_sql() unrecognized params type";
    }

    my $sql = "delete from $table\n$where";
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

# $delete_sql = $rep->_mk_delete_row_sql ($table, \@cols, \@row, \@keycolidx);
sub _mk_delete_row_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row, $keycolidx) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});
    my ($sql, $where, @colused, $col, $value, $colnum, $i, $nonkeycolnum, $quoted);
    if ($#$cols == -1) {
        $self->{error} = "Database->_mk_delete_row_sql(): no columns specified";
        return();
    }
    my $tabcols = $self->{table}{$table}{column};

    $colused[$#$cols] = 0;   # pre-extend the array

    $sql = "delete from $table\n";

    if (defined $keycolidx && $#$keycolidx > -1) {
        for ($i = 0; $i <= $#$keycolidx; $i++) {
            $colnum = $keycolidx->[$i];
            $col = $cols->[$colnum];
            if (!defined $row || $#$row == -1) {
                $value = "?";
            }
            else {
                $value = $row->[$colnum];
                if (!defined $value) {
                    $value = "NULL";
                }
                else {
                    $quoted = (defined $tabcols->{$col}{quoted}) ? ($tabcols->{$col}{quoted}) : ($value !~ /^-?[0-9.]+$/);
                    if ($quoted) {
                        $value =~ s/'/\\'/g;
                        $value = "'$value'";
                    }
                }
            }
            $where .= ($i == 0) ? "where $col = $value" : "\n  and $col = $value";
            $colused[$colnum] = 1;
        }
        $where .= "\n";
    }

    $sql .= $where;
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

# $delete_sql = $rep->_mk_delete_rows_sql($table, \@params, \%paramvalues);
sub _mk_delete_rows_sql {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $paramvalues) = @_;
    $self->_load_table_metadata($table) if (!defined $self->{table}{$table}{loaded});
    my ($sql);

    $sql = "delete from $table\n";
    $sql .= $self->_mk_where_clause($table, $params);
    &App::sub_exit($sql) if ($App::trace);
    $sql;
}

######################################################################
# SIMPLE SQL OPERATIONS
######################################################################

# $row = $rep->select_row ($table, \@cols, \@params, \%paramvalues);

# this is a new version that uses bind variables instead of relying on my quoting rules
# unfortunately, it doesn't work yet

sub _select_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $params, $paramvalues) = @_;
    my ($dbh, $sql, $param, @params, %paramvalues, @paramvalues);

    $self->{error} = "";

    if (defined $params) {
        @params = @$params;
    }
    else {
        @params = (keys %$paramvalues);
    }
    foreach $param (@params) {
        push(@paramvalues, $paramvalues->{$param});
    }

    if ($self->{table}{$table}{rawaccess}) {
        $sql = $self->_mk_select_sql($table, $cols, \@params, \%paramvalues, undef, 1, 1);
    }
    else {
        $sql = $self->_mk_select_rows_sql($table, $cols, \@params, \%paramvalues, undef, 1, 1);
    }
    $self->{sql} = $sql;

    my $rows = $self->_selectrange_arrayref($sql, 1, 1, undef, @paramvalues);
    if (!$rows || $#$rows == -1) {
        &App::sub_exit([]) if ($App::trace);
        return [];
    }
    &App::sub_exit($rows->[0]) if ($App::trace);
    return ($rows->[0]);
}

# NOTE: everything after the first line is optional
# @rows = $rep->_select_rows($table, \@cols,
#               \@params, \%paramvalues, \@order_by,
#               $startrow, $endrow,
#               \@sortdircol, \@keycolidx, \@writeable, \@columntype, \@summarykeys);
# TODO: get the $startrow/$endrow working when one/both/neither work in the SQL portion
# TODO: rethink $startrow/$endrow vs. $numrows/$skiprows

# this is a new version that uses bind variables instead of relying on my quoting rules
# unfortunately, it doesn't work yet

sub _select_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $params, $paramvalues, $order_by, $startrow, $endrow,
        $sortdircol, $keycolidx, $writeable, $columntype, $summarykeys) = @_;
    my ($sql, $param, @params, %paramvalues, @paramvalues);

    $self->{error} = "";

    if (defined $params) {
        @params = @$params;
    }
    else {
        @params = (keys %$paramvalues);
    }
    foreach $param (@params) {
        push(@paramvalues, $paramvalues->{$param});
    }

    if ($self->{table}{$table}{rawaccess}) {
        $sql = $self->_mk_select_sql($table, $cols, \@params, \%paramvalues, $order_by,
            $startrow, $endrow, $sortdircol, $keycolidx, $writeable, $columntype, $summarykeys);
    }
    else {
        $sql = $self->_mk_select_rows_sql($table, $cols, \@params, \%paramvalues, $order_by,
            $startrow, $endrow, $sortdircol, $keycolidx, $writeable, $columntype, $summarykeys);
    }
    $self->{sql} = $sql;
    my $retval = $self->_selectrange_arrayref($sql, $startrow, $endrow, undef, @paramvalues);
    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

# $ok = $rep->_insert_row($table, \@cols, \@row);
sub _insert_row {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $row) = @_;
    $self->{error} = "";
    my $sql = $self->_mk_insert_row_sql($table, $cols);
    $self->{sql} = $sql;
    my $dbh = $self->{dbh};
    my $retval = 0;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: insert()\n";
        print "DEBUG_SQL: bind vars [", join("|",map { defined $_ ? $_ : "undef" } @$row), "]\n";
        print $sql;
    }
    $retval = $dbh->do($sql, undef, @$row) if (defined $dbh);
    if ($debug_sql && $debug_sql >= 2) {
        print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
        print "\n";
    }

    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

# $ok = $rep->_insert_rows ($table, \@cols, \@rows);
sub _insert_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $cols, $rows) = @_;
    $self->{error} = "";
    my ($row, $sql, $nrows, $ok, $retval);
   
    my $dbh = $self->{dbh};
    return 0 if (!defined $dbh);

    $ok = 1;
    $sql = $self->_mk_insert_row_sql($table, $cols);
    my $debug_sql = $self->{context}{options}{debug_sql};
    foreach $row (@$rows) {
        $nrows += $self->{numrows};

        if ($debug_sql) {
            print "DEBUG_SQL: _insert_rows()\n";
            print "DEBUG_SQL: bind vars [", join("|",map { defined $_ ? $_ : "undef" } @$row), "]\n";
            print $sql;
        }
        $retval = $dbh->do($sql, undef, @$row);
        if ($debug_sql) {
            print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
            print "\n";
        }

        if (!$retval) {
            $self->{numrows} = $nrows;
            $ok = 0;
            last;
        }
    }
    $self->{sql} = $sql;
    $self->{numrows} = $nrows;
    &App::sub_exit($ok) if ($App::trace);
    return($ok);
}

sub _delete {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $row, $options) = @_;
    $self->{error} = "";
    my $sql = $self->_mk_delete_sql($table, $params, $cols, $row, $options);
    $self->{sql} = $sql;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _delete()\n";
        print $sql;
    }
    my $retval = $self->{dbh}->do($sql);
    if ($debug_sql) {
        print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
        print "\n";
    }

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
    my $sql = $self->_mk_update_sql($table, $params, $cols, $row, $options);
    $self->{sql} = $sql;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _update()\n";
        print $sql;
    }
    my $retval = $self->{dbh}->do($sql);
    if ($debug_sql) {
        print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
        print "\n";
    }

    &App::sub_exit($retval) if ($App::trace);
    return($retval);
}

# $ok = $rep->_delete_row ($table, \@cols, \@row, \@keycolidx);
sub _delete_row {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    $self->{error} = "";
    my $sql = $self->_mk_delete_row_sql(@_);
    $self->{sql} = $sql;
    my $dbh = $self->{dbh};
    my $retval = 0;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _delete_row()\n";
        print $sql;
    }
    $retval = $dbh->do($sql) if (defined $dbh);
    if ($debug_sql) {
        print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
        print "\n";
    }

    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

# $ok = $rep->_delete_rows($table, \@params, \%paramvalues);
sub _delete_rows {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    $self->{error} = "";
    my $sql = $self->_mk_delete_rows_sql(@_);
    $self->{sql} = $sql;
    my $dbh = $self->{dbh};
    my $retval = 0;

    my $debug_sql = $self->{context}{options}{debug_sql};
    if ($debug_sql) {
        print "DEBUG_SQL: _delete_rows()\n";
        print $sql;
    }
    $retval = $dbh->do($sql) if (defined $dbh);
    if ($debug_sql) {
        print "DEBUG_SQL: retval [$retval] $DBI::errstr\n";
        print "\n";
    }

    &App::sub_exit($retval) if ($App::trace);
    $retval;
}

######################################################################
# METADATA REPOSITORY METHODS (implements methods from App::Repository)
######################################################################

use DBIx::Compat;

sub _load_rep_metadata_from_source {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    my ($dbdriver, $dbh);
    $dbdriver = $self->{dbdriver};
    $dbh = $self->{dbh};

    #####################################################
    # TABLE DATA
    #####################################################

    my ($table, @tables, $func);

    # if we are not hiding the physical tables, go get them
    if (! $self->{hide_physical}) {

        # get a list of the physical tables from the database
        # in MySQL 4.0.13, the table names are surrounded by backticks (!?!)
        # so for safe measure, get rid of all quotes
        @tables = grep(s/['"`]//g, $dbh->tables);

        # if the DBI method doesn't work, try the DBIx method...
        if ($#tables == -1) {
            $func = DBIx::Compat::GetItem($dbdriver, "ListTables");
            @tables = &{$func}($dbh);
        }

        # go through the list of native tables from the database
        foreach $table (@tables) {

            # if it has never been defined, then define it
            if (!defined $self->{table}{$table}) {
                $self->{table}{$table} = {
                    "name" => $table,
                };
            }

            # if it has not been added to the list and it is not explicitly hidden, add to list
            if (!defined $self->{table}{$table}{idx} && ! $self->{table}{$table}{hide}) {
                push(@{$self->{tables}}, $table);                  # add to list
                $self->{table}{$table}{idx} = $#{$self->{tables}}; # take note of the index
            }
        }
    }

    #########################################################
    # TYPE DATA
    # note: these are native database types, whereas a Repository "type" is a standard
    #########################################################

    my ($ntype_attribute_idx, @ntype_attribute_values);
    ($ntype_attribute_idx, @ntype_attribute_values) = @{$dbh->type_info_all};

    # Contents of $type_attribute_idx for MySQL:
    # $ntype_attribute_idx = {
    #     "TYPE_NAME"          =>  0,
    #     "DATA_TYPE"          =>  1,
    #     "COLUMN_SIZE"        =>  2,
    #     "LITERAL_PREFIX"     =>  3,
    #     "LITERAL_SUFFIX"     =>  4,
    #     "CREATE_PARAMS"      =>  5,
    #     "NULLABLE"           =>  6,
    #     "CASE_SENSITIVE"     =>  7,
    #     "SEARCHABLE"         =>  8,
    #     "UNSIGNED_ATTRIBUTE" =>  9,
    #     "FIXED_PREC_SCALE"   => 10,
    #     "AUTO_UNIQUE_VALUE"  => 11,
    #     "LOCAL_TYPE_NAME"    => 12,
    #     "MINIMUM_SCALE"      => 13,
    #     "MAXIMUM_SCALE"      => 14,
    #     "NUM_PREC_RADIX"     => 15,
    #     "mysql_native_type"  => 16,
    #     "mysql_is_num"       => 17,
    # };

    # Contents of @ntype_attribute_values for MySQL:
    # TYPE_NAME   DATA_TYPE COLUMN_SIZE PRE SUF CREATEPARAMETERS NUL CASE SRCH UNS FIX AUTO LTYPE MINS MAXS RDX
    # varchar            12         255 '   '   max length         1    0    1   0   0    0 0        0    0   0
    # decimal             3          15         precision,scale    1    0    1   0   0    0 0        0    6   2
    # tinyint            -6           3                            1    0    1   0   0    0 0        0    0  10
    # smallint            5           5                            1    0    1   0   0    0 0        0    0  10
    # integer             4          10                            1    0    1   0   0    0 0        0    0  10
    # float               7           7                            1    0    0   0   0    0 0        0    2   2
    # double              8          15                            1    0    1   0   0    0 0        0    4   2
    # timestamp          11          14 '   '                      0    0    1   0   0    0 0        0    0   0
    # bigint             -5          20                            1    0    1   0   0    0 0        0    0  10
    # middleint           4           8                            1    0    1   0   0    0 0        0    0  10
    # date                9          10 '   '                      1    0    1   0   0    0 0        0    0   0
    # time               10           6 '   '                      1    0    1   0   0    0 0        0    0   0
    # datetime           11          21 '   '                      1    0    1   0   0    0 0        0    0   0
    # year                5           4                            1    0    1   0   0    0 0        0    0   0
    # date                9          10 '   '                      1    0    1   0   0    0 0        0    0   0
    # enum               12         255 '   '                      1    0    1   0   0    0 0        0    0   0
    # set                12         255 '   '                      1    0    1   0   0    0 0        0    0   0
    # blob               -1       65535 '   '                      1    0    1   0   0    0 0        0    0   0
    # tinyblob           -1         255 '   '                      1    0    1   0   0    0 0        0    0   0
    # mediumblob         -1    16777215 '   '                      1    0    1   0   0    0 0        0    0   0
    # longblob           -1  2147483647 '   '                      1    0    1   0   0    0 0        0    0   0
    # char                1         255 '   '   max length         1    0    1   0   0    0 0        0    0   0
    # decimal             2          15         precision,scale    1    0    1   0   0    0 0        0    6   2
    # tinyint unsigned   -6           3                            1    0    1   1   0    0 0        0    0  10
    # smallint unsigned   5           5                            1    0    1   1   0    0 0        0    0  10
    # middleint unsigned  4           8                            1    0    1   1   0    0 0        0    0  10
    # int unsigned        4          10                            1    0    1   1   0    0 0        0    0  10
    # int                 4          10                            1    0    1   0   0    0 0        0    0  10
    # integer unsigned    4          10                            1    0    1   1   0    0 0        0    0  10
    # bigint unsigned    -5          20                            1    0    1   1   0    0 0        0    0  10
    # text               -1       65535 '   '                      1    0    1   0   0    0 0        0    0   0
    # mediumtext         -1    16777215 '   '                      1    0    1   0   0    0 0        0    0   0

    my ($ntype_name, @ntype_names, $ntype_num, $ntype_attribute_values, $ntype_def);
    my ($ntype_name_idx, $ntype_num_idx, $column_size_idx, $literal_prefix_idx, $literal_suffix_idx);
    my ($unsigned_attribute_idx, $auto_unique_value_idx, $column);

    $ntype_name_idx         = $ntype_attribute_idx->{"TYPE_NAME"};
    $ntype_num_idx          = $ntype_attribute_idx->{"DATA_TYPE"};
    $column_size_idx        = $ntype_attribute_idx->{"COLUMN_SIZE"};
    $literal_prefix_idx     = $ntype_attribute_idx->{"LITERAL_PREFIX"};
    $literal_suffix_idx     = $ntype_attribute_idx->{"LITERAL_SUFFIX"};
    $unsigned_attribute_idx = $ntype_attribute_idx->{"UNSIGNED_ATTRIBUTE"};
    $auto_unique_value_idx  = $ntype_attribute_idx->{"AUTO_UNIQUE_VALUE"};

    # go through the list of native type info from the DBI handle
    foreach $ntype_attribute_values (@ntype_attribute_values) {

        $ntype_name = $ntype_attribute_values->[$ntype_name_idx];
        $ntype_num = $ntype_attribute_values->[$ntype_num_idx];
        $ntype_def = {};
        push(@ntype_names, $ntype_name);

        $self->{native}{type}{$ntype_name} = $ntype_def;
        if (!defined $self->{native}{type}{$ntype_num}) {
            $self->{native}{type}{$ntype_num} = $ntype_def;
        }

        # save all the info worth saving in a native type definition
        $ntype_def->{name}               = $ntype_name;  # a real type name
        $ntype_def->{num}                = $ntype_num;  # an internal data type number
        $ntype_def->{column_size}        = $ntype_attribute_values->[$column_size_idx];
        $ntype_def->{literal_prefix}     = $ntype_attribute_values->[$literal_prefix_idx];
        $ntype_def->{literal_suffix}     = $ntype_attribute_values->[$literal_suffix_idx];
        $ntype_def->{unsigned_attribute} = $ntype_attribute_values->[$unsigned_attribute_idx];
        $ntype_def->{auto_unique_value}  = $ntype_attribute_values->[$auto_unique_value_idx];
        $ntype_def->{literal_prefix}     = "" if (! defined $ntype_def->{literal_prefix});
        $ntype_def->{literal_suffix}     = "" if (! defined $ntype_def->{literal_suffix});

        $ntype_def->{quoted} = ($ntype_def->{literal_prefix} ne "" || $ntype_def->{literal_suffix} ne "");

        # translate a native type into a repository type

        if ($ntype_name =~ /char/ || $ntype_name eq "enum" || $ntype_name eq "set") {
            $ntype_def->{type} = "string";
        }
        elsif ($ntype_name =~ /text/) {
            $ntype_def->{type} = "text";
        }
        elsif ($ntype_name =~ /int/ || $ntype_name eq "year") {
            $ntype_def->{type} = "integer";
        }
        elsif ($ntype_name =~ /decimal/ || $ntype_name =~ /float/ || $ntype_name =~ /double/) {
            $ntype_def->{type} = "float";
        }
        elsif ($ntype_name =~ /datetime/ || $ntype_name =~ /timestamp/) {
            $ntype_def->{type} = "datetime";
        }
        elsif ($ntype_name =~ /time/) {
            $ntype_def->{type} = "time";
        }
        elsif ($ntype_name =~ /date/) {
            $ntype_def->{type} = "date";
        }
        elsif ($ntype_name =~ /blob/ || $ntype_name =~ /binary/) {
            $ntype_def->{type} = "binary";
        }
    }

    $self->{native}{types} = \@ntype_names;

    #########################################################
    # DATABASE ATTRIBUTES
    #########################################################
    $self->{native}{support_join}           = DBIx::Compat::GetItem($dbdriver, "SupportJoin");
    $self->{native}{inner_join_syntax}      = DBIx::Compat::GetItem($dbdriver, "SupportSQLJoin");
    $self->{native}{inner_join_only2tables} = DBIx::Compat::GetItem($dbdriver, "SQLJoinOnly2Tabs");
    $self->{native}{have_types}             = DBIx::Compat::GetItem($dbdriver, "HaveTypes");
    $self->{native}{null_operator}          = DBIx::Compat::GetItem($dbdriver, "NullOperator");
    $self->{native}{need_null_in_create}    = DBIx::Compat::GetItem($dbdriver, "NeedNullInCreate");
    $self->{native}{empty_is_null}          = DBIx::Compat::GetItem($dbdriver, "EmptyIsNull");

    &App::sub_exit() if ($App::trace);
}

sub _load_table_metadata_from_source {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;

    return if (! $table);

    my ($dbdriver, $dbh, $sth, $native_table, $table_def);
    my (@tables, $column, $func, $tablealias);

    $dbdriver = $self->{dbdriver};
    $dbh = $self->{dbh};
    $table_def = $self->{table}{$table};
    return if (!defined $table_def);

    $native_table = $table;     # assume the table name is a physical one
    $native_table = $table_def->{native_table} if ($table_def->{native_table});

    $table_def->{name} = $table;

    $tablealias = $table_def->{alias};
    if (! defined $tablealias) {
        $tablealias = "t" . $self->serial("table");
        $table_def->{alias} = $tablealias;
    }

    $table_def->{tablealiases} = [ $tablealias ]
        if (!defined $table_def->{tablealiases});
    $table_def->{tablealias} = {}
        if (!defined $table_def->{tablealias});
    $table_def->{tablealias}{$tablealias} = {}
        if (!defined $table_def->{tablealias}{$tablealias});
    $table_def->{tablealias}{$tablealias}{table} = $table
        if (!defined $table_def->{tablealias}{$tablealias}{table});

    #########################################################
    # COLUMN DATA
    #########################################################
    my ($colnum, $data_types, $columns, $column_def, $phys_columns);
    my ($native_type_num, $native_type_def, $phys_table);

    $func = DBIx::Compat::GetItem($dbdriver, "ListFields");
    eval {
        $sth  = &{$func}($dbh, $table);
    };
    if (!$@) {
        $table_def->{phys_table} = $table;
        $phys_columns = $sth->{NAME};    # array of fieldnames
        $data_types   = $sth->{TYPE};    # array of fieldtypes

        $columns = $table_def->{columns};
        if (! defined $columns) {
            $columns = [];
            $table_def->{columns} = $columns;
        }

        # if we got a list of columns for the table from the database
        if (defined $phys_columns && ref($phys_columns) eq "ARRAY") {

            for ($colnum = 0; $colnum <= $#$phys_columns; $colnum++) {
                $column = $phys_columns->[$colnum];

                $column_def = $table_def->{column}{$column};
                if (!defined $column_def) {
                    $column_def = {};
                    $table_def->{column}{$column} = $column_def;
                }
                next if ($column_def->{hide});

                $native_type_num = $data_types->[$colnum];
                $native_type_def = $self->{native}{type}{$native_type_num};

                if (! $self->{hide_physical} && ! defined $column_def->{idx}) {
                    push(@$columns, $column);
                    $column_def->{idx} = $#$columns;
                }

                $column_def->{name}   = $column;
                $column_def->{type}   = $native_type_def->{type};
                $column_def->{quoted} = $native_type_def->{quoted} ? 1 : 0;
                $column_def->{alias}  = "cn$colnum" if (!defined $column_def->{alias});
                $column_def->{dbexpr} = $table_def->{alias} . "." . $column
                    if (!defined $column_def->{dbexpr});
            }
        }
    }

    ######################################################################
    # primary key
    ######################################################################

    if (!$self->{primary_key} || !$self->{alternate_key}) {
        $self->_load_table_key_metadata($table);
    }

    ######################################################################
    # tables that are related via tablealiases can be "import"-ed
    # this copies all of the column definitions from the imported table to this table
    # TODO: allow for role modifiers in related tables
    # TODO: rethink "import=1" to "multiplicity=1"
    # TODO: think about chained imports
    # TODO: think about import on demand rather than in advance
    ######################################################################
    my ($tablealiases, $alias, $alias_def, $related_table, $related_table_def);
    my ($tablealias_defs, $tablealias_def, $idx);

    $tablealiases = $table_def->{tablealiases};
    if (defined $tablealiases && ref($tablealiases) eq "ARRAY") {
        foreach $alias (@$tablealiases) {
            $alias_def = $table_def->{tablealias}{$alias};
            if ($alias_def->{import}) {
                $related_table = $alias_def->{table};
                if (! $self->{table}{$related_table}{loaded}) {
                    $self->_load_table_metadata($related_table);
                }
                $related_table_def = $self->{table}{$related_table};
                foreach $column (@{$related_table_def->{columns}}) {
                    if (! defined $table_def->{column}{$column} &&
                          defined $related_table_def->{column}{$column}) {
                        $table_def->{column}{$column} = $related_table_def->{column}{$column};
                    }
                }
            }
        }
    }

    # for each tablealias named in the configuration, give it a number up front
    $tablealias_defs = $table_def->{tablealias};
    for ($idx = 0; $idx <= $#$tablealiases; $idx++) {
        $tablealias = $tablealiases->[$idx];
        $tablealias_defs->{$tablealias}{idx} = $idx;
    }

    # for each tablealias in the hash (random order), add them to the end
    foreach $tablealias (keys %$tablealias_defs) {
        $tablealias_def = $tablealias_defs->{$tablealias};

        # table has not been added to the list and it's not explicitly "hidden", so add it
        if (!defined $tablealias_def->{idx}) {
            push(@$tablealiases, $tablealias);
            $tablealias_def->{idx} = $#$tablealiases;
        }
    }

    #if ($App::DEBUG >= 2 && $self->{context}->dbg(2)) {
    #    print "Table Metadata: $table\n";
    #    my $d = Data::Dumper->new([ $table_def ], [ "table_def" ]);
    #    $d->Indent(1);
    #    print $d->Dump();
    #}
    &App::sub_exit() if ($App::trace);
}

sub _load_table_key_metadata {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;

    return if (! $table);
    my $table_def = $self->{table}{$table};
    return if (! $table_def);
    my $dbh = $self->{dbh};

    # if not defined at all, try to get it from the database
    if (! defined $table_def->{primary_key}) {
        eval {
            $table_def->{primary_key} = [ $dbh->primary_key($self->{dbcatalog}, $self->{dbschema}, $table) ];
        };
    }
    &App::sub_exit() if ($App::trace);
}

1;

