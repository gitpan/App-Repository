
#############################################################################
## $Id: RepositoryObjectSet.pm,v 1.1 2002/10/12 03:03:48 spadkins Exp $
#############################################################################

package App::RepositoryObjectSet;

use App;
use App::Repository;
use App::SessionObject;

@ISA = ( "App::SessionObject" );

use strict;

=head1 NAME

App::RepositoryObjectSet - A set of repository objects described by a set of query parameters

=head1 SYNOPSIS

    use App::RepositoryObjectSet;

    ...

=cut

=head1 DESCRIPTION

A RepositoryObjectSet is a set of repository objects (i.e. rows in 
a database).

All RepositoryObjectSet are created using the $rep->object() method,
and they all have the following attributes.

    $self->{_repository} - the Repository which the object came from
    $self->{_context}    - the Context the object is running in
    $self->{_table}      - the table name associated with the object
    $self->{_key}        - the unique identifier of the object in the
                           table in the repository

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
    * Return:    void
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

