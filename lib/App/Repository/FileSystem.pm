
######################################################################
## File: $Id: File.pm,v 1.1 2003/06/27 18:39:37 spadkins Exp $
######################################################################

use App;
use App::Repository;

package App::Repository::FileSystem;
$VERSION = do { my @r=(q$Revision: 1.1 $=~/\d+/g); sprintf "%d."."%02d"x$#r,@r};

@ISA = ( "App::Repository" );

use Data::Dumper;
use App::Reference;
use File::Spec;

use strict;

=head1 NAME

App::Repository::File - a repository which stores its data in flat files

=head1 SYNOPSIS

   use App::Repository::File;

   (see man page for App::Repository for methods)

=cut

=head1 DESCRIPTION

The App::Repository::FileSystem class encapsulates all access to files
on a file system as though they were in a relational data store.
It implements access to a few logical tables: file and filesystem.

It is different than App::Repository::File which stores data in files.
Rather, App::Repository::FileSystem gives access to all file system
data as though it were in a single table.

=cut

sub _get_rows {
    &App::sub_entry if ($App::trace);
    my ($self, $table, $params, $cols, $options) = @_;
    my $rows = [];

    if ($table eq "file") {
        $rows = $self->_get_rows_file($params, $cols, $options);
    }

    &App::sub_exit($rows) if ($App::trace);
    return($rows);
}

######################################################################
# attributes
######################################################################
# PK   path       "/index.html"     "C:\htdocs\index.htm"
#      name       "index.html"      "index.htm"
#      directory  "/"               "\htdocs"
#      volume     ""                "C:"
######################################################################
#      data
######################################################################
# stat($filename)
#    0 dev      device number of filesystem
#    1 ino      inode number
#    2 mode     file mode  (type and permissions)
#    3 nlink    number of (hard) links to the file
#    4 uid      numeric user ID of file's owner
#    5 gid      numeric group ID of file's owner
#    6 rdev     the device identifier (special files only)
#    7 size     total size of file, in bytes
#    8 atime    last access time in seconds since the epoch
#    9 mtime    last modify time in seconds since the epoch
#   10 ctime    inode change time in seconds since the epoch (*)
#   11 blksize  preferred block size for file system I/O
#   12 blocks   actual number of blocks allocated
######################################################################

sub _get_rows_file {
    &App::sub_entry if ($App::trace);
    my ($self, $params, $cols, $options) = @_;
    my $table = "file";
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

sub _load_rep_metadata_from_source {
    &App::sub_entry if ($App::trace);
    my ($self) = @_;

    # ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
    #     $atime,$mtime,$ctime,$blksize,$blocks)
    #         = stat($filename);
    my $config = {
      auto_label => 1,
      table => {
        file => {
          primary_key => [ "file_path" ],
          columns => [qw(file_path file_catalog file_dir file_name file_owner file_group create_dttm modified_dttm
                         size dev ino mode nlink uid gid rdev size atime mtime ctime blksize blocks)],
          column => {
            file_path => {
            },
            file_catalog => {
            },
            file_dir => {
            },
            file_name => {
            },
            file_owner => {
            },
            file_group => {
            },
            create_dttm => {
            },
            modified_dttm => {
            },
            size => {
            },
            dev => {
            },
            ino => {
            },
            mode => {
            },
            nlink => {
            },
            uid => {
            },
            gid => {
            },
            rdev => {
            },
            size => {
            },
            atime => {
            },
            mtime => {
            },
            ctime => {
            },
            blksize => {
            },
            blocks => {
            },
            data => {
            },
          },
        },
      },
    };

    App::Reference->overlay($self, $config);

    &App::sub_exit() if ($App::trace);
}

1;

