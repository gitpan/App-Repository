#!/usr/local/bin/perl -w

use App::Options;

use Test::More qw(no_plan);
use lib "../App-Context/lib";
use lib "../../App-Context/lib";
use lib "lib";
use lib "../lib";

use App;
use App::Repository;
use strict;

my $root = (-d "t") ? "." : "..";

my $context = App->context(
    conf_file => "",
    conf => {
        Repository => {
            default => {
                class => "App::Repository::FileSystem",
                root => $root,
            },
        },
    },
);

my $rep = $context->repository();

{
    is(ref($rep), "App::Repository::FileSystem", "isa App::Repository::FileSystem");
    my $columns = $rep->get_column_names("file");
    my @file_columns = qw(file_path file_catalog file_dir file_name file_owner file_group create_dttm modified_dttm
        size dev ino mode nlink uid gid rdev size atime mtime ctime blksize blocks data);
    is_deeply($columns, \@file_columns, "get_column_names() returns file columns");
    my $labels = $rep->get_column_labels("file");
    is($labels->{file_path}, "File Path", "get_column_labels(file)->{file_path}");
exit(0);
}

my ($file1, $data1, $file2, $data2);

{
    local(*FILE);
    mkdir("$root/tmp") if (! -d "$root/tmp");
    open(main::FILE, "> $root/tmp/file1");
    $file1 = "/tmp/file1";
    $file2 = "/tmp/x/file2";
    $data1 = <<EOF;
This is test data.
EOF
    $data2 = <<EOF;
This is more test data.
EOF
    print main::FILE $data1;
    close(main::FILE);
}

{
    my $data1b = $rep->get("file",$file1,"data");
    is($data1b, $data1, "get() data");
}

exit 0;

