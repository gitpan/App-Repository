
#############################################################################
## $Id: RepositoryObjectSet.pm,v 1.5 2005/03/31 20:04:01 spadkins Exp $
#############################################################################

package App::SessionObject::RepositoryObjectSet;

use App;
use App::Repository;
use App::SessionObject;

@ISA = ( "App::SessionObject" );

use strict;

=head1 NAME

App::SessionObject::RepositoryObjectSet - A set of repository objects described by a set of query parameters

=head1 SYNOPSIS

    use App::SessionObject::RepositoryObjectSet;

    ...

=cut

=head1 DESCRIPTION

A RepositoryObjectSet is a set of repository objects (i.e. rows in 
a database).

By using a RepositoryObjectSet instead of simply doing a query, you get
a variety of benefits.

 * session-level caching
 * find domains of given columns (get_column_values())
 * create unique and non-unique indexes of the object set based on
   groups of columns (get_index(), get_unique_index())
 * efficiently fetch single objects within the set or subsets of objects
   which share common values in a set of attributes

=cut

###########################################################################
# Support Routines
###########################################################################

sub _clear_cache {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;
    delete $self->{objects};
    delete $self->{index};
    delete $self->{unique_index};
    delete $self->{column_values};
    delete $self->{max_age_time};
    &App::sub_exit() if ($App::trace);
}

sub _clear_cache_if_objects_expired {
    &App::sub_entry if ($App::trace);
    my ($self, $options) = @_;
    if (defined $options->{max_age} && $self->{objects}) {
        my $max_age = $options->{max_age};
        my $max_age_time = $self->{max_age_time};
        my $time = time();
        if (defined $max_age_time && $max_age_time <= $time - $max_age) {
            $self->_clear_cache();
        }
    }
    &App::sub_exit() if ($App::trace);
}

sub set_table {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $repository) = @_;
    $self->{repository} = $repository || "default";
    $self->{table} = $table;
    $self->_clear_cache();
    &App::sub_exit() if ($App::trace);
}

sub set_params {
    &App::sub_entry if ($App::trace);
    my ($self, $params) = @_;
    $params ||= {};
    $self->{params} = { %$params };
    $self->_clear_cache();
    &App::sub_exit() if ($App::trace);
}

sub update_params {
    &App::sub_entry if ($App::trace);
    my ($self, $params) = @_;
    my $self_params = $self->{params};
    die "params must be set before update_params() is called" if (!$self_params);
    my $param_changed = 0;
    foreach my $key (keys %$self_params) {
        if (exists $params->{$key} &&
            $self_params->{$key} ne $params->{$key}) {
            $self_params->{$key} = $params->{$key};
            $param_changed = 1;
        }
    }
    if ($param_changed && $self->{objects}) {
        $self->_clear_cache();
    }
    &App::sub_exit() if ($App::trace);
}

sub _get_all_objects {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;
    my $objects = $self->{objects};
    if (!$objects) {
        my $context = $self->{context};
        my $repname = $self->{repository};
        my $rep     = $context->repository($repname);
        my $table   = $self->{table} || die "table not defined";
        my $params  = $self->{params} || {};
        $objects = $rep->get_objects($table, $params);
        $self->{objects} = $objects;
        $self->{max_age_time} = time();
    }
    &App::sub_exit($objects) if ($App::trace);
    return($objects);
}

###########################################################################
# Accessing individual objects
###########################################################################

sub get_index {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my $options = (ref($_[0]) eq "HASH") ? shift : {};
    my $key_name = ref($_[0]) ? "ie1" : shift;
    $key_name ||= "ie1";
    my $key_columns = shift;

    $self->_clear_cache_if_objects_expired($options) if (defined $options->{max_age} && $self->{objects});

    my $index = $self->{index}{$key_name};
    if (!$index) {
        if ($self->{key}{$key_name}) {
            $key_columns = $self->{key}{$key_name};
        }
        die "no list of columns given or known for key [$key_name]" if (!$key_columns);
        my ($key);
        $index = {};
        my $objects = $self->_get_all_objects();
        foreach my $object (@$objects) {
            $key = join(",", @{$object}{@$key_columns});
            if ($index->{$key}) {
                push(@{$index->{$key}}, $object);
            }
            else {
                $index->{$key} = [ $object ];
            }
        }
        $self->{index}{$key_name} = $index;
    }
    &App::sub_exit($index) if ($App::trace);
    return($index);
}

# $self->get_unique_index($key_columns);
# $self->get_unique_index($key_name, $key_columns);
# $self->get_unique_index($key_name);
sub get_unique_index {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my $options = (ref($_[0]) eq "HASH") ? shift : {};
    my $key_name = ref($_[0]) ? "ak1" : shift;
    $key_name ||= "ak1";
    my $key_columns = shift;

    $self->_clear_cache_if_objects_expired($options) if (defined $options->{max_age} && $self->{objects});

    my $unique_index = $self->{unique_index}{$key_name};
    if (!$unique_index) {
        if ($self->{key}{$key_name}) {
            $key_columns = $self->{key}{$key_name};
        }
        die "no list of columns given or known for key [$key_name]" if (!$key_columns);
        my ($key);
        $unique_index = {};
        my $objects = $self->_get_all_objects();
        foreach my $object (@$objects) {
            $key = join(",", @{$object}{@$key_columns});
            $unique_index->{$key} = $object;
        }
        $self->{unique_index}{$key_name} = $unique_index;
    }
    &App::sub_exit($unique_index) if ($App::trace);
    return($unique_index);
}

sub get_column_values {
    &App::sub_entry if ($App::trace);
    my ($self, $column, $options) = @_;

    $self->_clear_cache_if_objects_expired($options) if (defined $options->{max_age} && $self->{objects});

    my $values = $self->{column_values}{$column};
    if (!$values) {
        $values = [];
        my $objects = $self->_get_all_objects();
        my (%count, $value);
        foreach my $object (@$objects) {
            $value = $object->{$column};
            if (!$count{$value}) {
                $count{$value} = 1;
                push(@$values, $value);
            }
            else {
                $count{$value} ++;
            }
        }
        $self->{column_values}{$column} = $values;
    }
    &App::sub_exit($values) if ($App::trace);
    return($values);
}

# $self->get_object($options, $key, $key_columns);
# $self->get_object($key, $key_columns);
# $self->get_object($key, $key_name, $key_columns);
# $self->get_object($key, $key_name);
sub get_object {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my $options = (ref($_[0]) eq "HASH") ? shift : {};
    my $key = shift;
    my $key_name = ref($_[0]) ? "ak1" : shift;
    my $key_columns = shift;

    $self->_clear_cache_if_objects_expired($options) if (defined $options->{max_age} && $self->{objects});

    my $unique_index = $self->get_unique_index($key_name, $key_columns);
    my $object = $unique_index->{$key};
    &App::sub_exit($object) if ($App::trace);
    return($object);
}

# $self->get_objects($key, $options, $key_name);
# $self->get_objects($key, $key_columns);
# $self->get_objects($key, $key_name, $key_columns);
# $self->get_objects($key, $key_name);
sub get_objects {
    &App::sub_entry if ($App::trace);
    my $self = shift;
    my $options = (ref($_[0]) eq "HASH") ? shift : {};
    my $key = shift;
    die "key not scalar" if (ref($key));
    my $key_name = ref($_[0]) ? "ie1" : shift;
    my $key_columns = shift;

    $self->_clear_cache_if_objects_expired($options) if (defined $options->{max_age} && $self->{objects});

    my ($objects);
    if ($key) {
        $key_name ||= "ie1";
        my $index = $self->get_index($key_name, $key_columns);
        $objects = $index->{$key} || [];
    }
    else {
        $objects = $self->_get_all_objects();
    }
    &App::sub_exit($objects) if ($App::trace);
    return($objects);
}

=head1 ACKNOWLEDGEMENTS

 * Author:  Stephen Adkins <stephen.adkins@officevision.com>
 * License: This is free software. It is licensed under the same terms as Perl itself.

=head1 SEE ALSO

L<C<App::Context>|App::Context>,
L<C<App::Repository>|App::Repository>

=cut

1;

