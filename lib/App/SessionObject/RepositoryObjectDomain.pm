
#############################################################################
## $Id: RepositoryObjectDomain.pm,v 1.4 2005/08/09 18:46:22 spadkins Exp $
#############################################################################

package App::SessionObject::RepositoryObjectDomain;

use App;
use App::Repository;
use App::SessionObject;

@ISA = ( "App::SessionObject" );

use strict;

use Date::Format;
use Date::Parse;

=head1 NAME

App::RepositoryObjectDomain - A domain of repository object sets bounded by a set of query parameters

=head1 SYNOPSIS

    use App::RepositoryObjectDomain;

    ...

=cut

=head1 DESCRIPTION

A RepositoryObjectDomain is a domain of repository object sets bounded by
a set of query parameters

=cut

###########################################################################
# Support Routines
###########################################################################

sub _clear_cache {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;
    my (@tables);
    if ($table) {
        @tables = ($table);
    }
    else {
        my $object_set = $self->{object_set};
        if (ref($object_set) eq "HASH") {
            foreach my $table (keys %$object_set) {
                if ($object_set->{$table}{gotten}) {
                    delete $object_set->{$table}{gotten};
                    push(@tables, $table);
                }
            }
        }
    }
    my $context = $self->{context};
    my ($object_set_name, $object_set);
    foreach my $table (@tables) {
        $object_set_name = $self->{object_set}{$table}{name} || "$self->{name}-$table";
        $object_set = $context->session_object($object_set_name);
        $object_set->_clear_cache();
    }
    &App::sub_exit() if ($App::trace);
}

sub set_params {
    &App::sub_entry if ($App::trace);
    my ($self, $params) = @_;
    $params ||= {};
    $self->{params} = { %$params };
    &App::sub_exit() if ($App::trace);
}

sub get_object_set {
    &App::sub_entry if ($App::trace);
    my ($self, $table) = @_;
    my $context = $self->{context};
    my $params = $self->{params} || {};

    my $object_set_name = $self->{object_set}{$table}{name} || "$self->{name}-$table";
    my $args = $self->{object_set}{$table}{args} || {};
    if (!$args->{class}) {
        $args->{class} = "App::SessionObject::RepositoryObjectSet";
    }
    if (!$args->{table}) {
        $args->{table} = $table;
    }
    if (!$args->{params}) {
        $args->{params} = $params;
    }
    my $object_set = $context->session_object($object_set_name, %$args);
    $self->{object_set}{$table}{gotten} = 1;

    $object_set->update_params($params);
    &App::sub_exit($object_set) if ($App::trace);
    return($object_set);
}

sub get_param_domain {
    my ($self, $param) = @_;
    my $domain = [];
    my $params = $self->{params};
    if ($params) {
        if (defined $params->{$param}) {
            $domain = [ split(/,/,$params->{$param}) ];
        }
        elsif (defined $params->{"begin_${param}"} && defined $params->{"end_${param}"}) {
            my $value = $params->{"begin_${param}"};
            my $end_value = $params->{"end_${param}"};
            if ($value =~ /^\d+$/) {
                $domain = [ ($value .. $end_value) ];
            }
            elsif ($value =~ /^\d{4}-\d\d-\d\d$/) {
                my $time = str2time($value) + 2*3600;
                while ($value le $end_value) {
                    push(@$domain, $value);
                    $time += 24*3600;
                    $value = time2str("%Y-%m-%d", $time);
                }
            }
        }
    }
    return($domain);
}

sub get_unique_values {
    my ($self, $column, $values, $value_idx, $value_count) = @_;
}

=head1 ACKNOWLEDGEMENTS

 * Author:  Stephen Adkins <stephen.adkins@officevision.com>
 * License: This is free software. It is licensed under the same terms as Perl itself.

=head1 SEE ALSO

L<C<App::Context>|App::Context>,
L<C<App::Repository>|App::Repository>

=cut

1;

