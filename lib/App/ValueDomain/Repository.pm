
#############################################################################
## $Id: Repository.pm,v 1.2 2005/01/07 13:39:40 spadkins Exp $
#############################################################################

package App::ValueDomain::Repository;

use App;
use App::ValueDomain;
@ISA = ( "App::ValueDomain" );

use strict;

=head1 NAME

App::ValueDomain - a set of values and their labels

=head1 SYNOPSIS

    use App;

    $context = App->context();
    $dom = $context->service("ValueDomain");
    $dom = $context->value_domain();

=head1 DESCRIPTION

A ValueDomain service represents a set of values and their labels.

=cut

#############################################################################
# METHODS
#############################################################################

=head1 Methods:

=cut

#############################################################################
# _load()
#############################################################################

=head2 _load()

The _load() method is called to get the list of valid values in a data
domain and the labels that should be used to represent these values to
a user.

    * Signature: $self->_load()
    * Signature: $self->_load($values_string)
    * Param:     $values_string    string
    * Return:    void
    * Throws:    App::Exception
    * Since:     0.01

    Sample Usage: 

    $self->_load();

=cut

sub _load {
    my ($self, $values_string) = @_;
    my ($rep);
    my ($values, $labels, $needs_loading, $refresh_interval, $time);
    my ($method, $args, $rows, $row);

    $values_string ||= "";

    $values = $self->{values};
    $labels = $self->{labels};

    $needs_loading = 0;

    # if this is a repository-based domain, and we have never loaded
    # the values/labels (or it's time to refresh them by loading them again)
    # then the values/labels indeed need loading
    my $repository  = $self->{repository};
    if (defined $repository && $repository ne "") {                    # repository-based domain
        if (!defined $values || !defined $labels ||                    # never loaded them yet
            (!$values_string && $self->{values_string}) ||   # asking for the whole domain, only subset loaded
            (defined $values_string && defined $self->{values_string} &&  # asking for a different subset than is loaded
             $values_string ne $self->{values_string})) {
            $needs_loading = 1;
        }
        else {
            $refresh_interval = $self->{refresh_interval};
            if (defined $refresh_interval && $refresh_interval ne "" && $refresh_interval >= 0) {
                if ($refresh_interval == 0) {
                    $needs_loading = 1;
                }
                else {
                    if (time() >= $self->{time} + $refresh_interval) {
                        $needs_loading = 1;
                    }
                }
            }
        }
    }

    if ($needs_loading) {
        my $context     = $self->{context};
        my $rep         = $context->repository($repository);
        my $table       = $self->{table};
        my $valuecolumn = $self->{valuecolumn};
        my $labelcolumn = $self->{labelcolumn};
        my $params      = $self->{params} || {};
        my %params      = %$params;
        $params{$valuecolumn} = $values_string if (defined $values_string && $values_string ne "");

        if ($rep && $table && $valuecolumn && $labelcolumn && $params) {
            $rows   = $rep->get_rows($table, \%params, [ $valuecolumn, $labelcolumn ]);
            $values = [];
            $labels = {};
            foreach $row (@$rows) {
                push(@$values, $row->[0]);
                $labels->{$row->[0]} = $row->[1];
            }
            $self->{values} = $values;
            $self->{labels} = $labels;
            $time = time();
            $self->{time} = $time;
            $self->{values_string} = $values_string;
        }
    }

    $values = [] if (! defined $values);
    $labels = {} if (! defined $labels);

    if (defined $values_string && $values_string ne "") {
        return($labels);
    }
    else {
        return($values,$labels);
    }
}

=head1 ACKNOWLEDGEMENTS

 * Author:  Stephen Adkins <stephen.adkins@officevision.com>
 * License: This is free software. It is licensed under the same terms as Perl itself.

=head1 SEE ALSO

L<C<App::Context>|App::Context>,
L<C<App::Service>|App::Service>

=cut

1;
