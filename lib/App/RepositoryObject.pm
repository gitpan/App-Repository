
#############################################################################
## $Id: RepositoryObject.pm,v 1.2 2004/09/02 21:01:03 spadkins Exp $
#############################################################################

package App::RepositoryObject;

use App;
use App::Repository;

use strict;

=head1 NAME

App::RepositoryObject - Interface for data persistence

=head1 SYNOPSIS

    use App::RepositoryObject;

    ...

=cut

=head1 DESCRIPTION

A RepositoryObject is an object whose state is stored in a repository.
It is a base class for many business classes.

All RepositoryObjects are created using the $rep->get_object() or
$rep->get_objects() methods,
and they all have the following attributes.

    $self->{_repository} - the Repository which the object came from
    $self->{_table}      - the table name associated with the object
    $self->{_key}        - the unique identifier of the object in the
                           table in the repository

I am considering adding the following standard attribute, but I have
not yet decided whether I should.

    $self->{_context}    - the Context the object is running in

=cut

#############################################################################
# PUBLIC METHODS
#############################################################################

=head1 Public Methods

=cut

#############################################################################
# get()
#############################################################################

=head2 get()

    * Signature: $value = $obj->get($attrib);
    * Signature: $value = $obj->get($attrib, $options);
    * Param:     $attrib      string,ARRAY
    * Return:    $value       any,ARRAY
    * Throws:    App::Exception
    * Since:     0.01

    Sample Usage: 

    $value = $obj->get($attrib);

Gets the value of one or more attributes of an object.

=cut

sub get {
    my ($self, $attrib, $options) = @_;
    my (@values, $value);
    if (ref($attrib) eq "ARRAY") {
        foreach my $column (@$attrib) {
            if (exists $self->{$attrib}) {
                push(@values, $self->{$attrib});
            }
            else {
                @values = ();
                last;
            }
        }
        if ($#values == -1) {
            @values = $self->{_repository}->get($self->{_table}, $self->{_key}, $attrib);
            for (my $i = 0; $i <= $#$attrib; $i++) {
                $self->{$attrib->[$i]} = $values[$i];
            }
        }
        return(@values);
    }
    else {
        if (exists $self->{$attrib}) {
            $value = $self->{$attrib};
        }
        else {
            $value = $self->{_repository}->get($self->{_table}, $self->{_key}, $attrib);
            $self->{$attrib} = $value;
        }
        return($value);
    }
}

#############################################################################
# set()
#############################################################################

=head2 set()

    * Signature: $obj->set($attrib, $value);
    * Signature: $obj->set($attrib, $value, $options);
    * Param:     $attrib      string,ARRAY
    * Param:     $value       any,ARRAY
    * Param:     $options     any,ARRAY
    * Throws:    App::Exception
    * Since:     0.01

    Sample Usage: 

    $obj->set($attrib, $value);

Sets the value of one or more attributes of an object.

=cut

sub set {
    my ($self, $attrib, $value, $options) = @_;
    my $nrows = $self->{_repository}->set($self->{_table}, $self->{_key}, $attrib, $value);
    if (ref($attrib) eq "ARRAY") {
        for (my $i = 0; $i <= $#$attrib; $i++) {
            if ($nrows && exists $self->{$attrib->[$i]}) {
                $self->{$attrib->[$i]} = $value->[$i];
            }
        }
    }
    else {
        if ($nrows && exists $self->{$attrib}) {
            $self->{$attrib} = $value;
        }
    }
    die "can't set($attrib, $value) on object[$self->{_table}.$self->{_key}]" if (!$nrows);
    return($nrows);
}

=head1 ACKNOWLEDGEMENTS

 * Author:  Stephen Adkins <stephen.adkins@officevision.com>
 * License: This is free software. It is licensed under the same terms as Perl itself.

=head1 SEE ALSO

L<C<App::Context>|App::Context>,
L<C<App::Repository>|App::Repository>

=cut

1;

