#!/usr/local/bin/perl -w

use App::Options (
    options => [qw(dbdriver dbclass dbhost dbname dbuser dbpass)],
    option => {
        dbclass  => { default => "App::Repository::MySQL", },
        dbdriver => { default => "mysql", },
        dbhost   => { default => "localhost", },
        dbname   => { default => "test", },
        dbuser   => { default => "scott", },
        dbpass   => { default => "tiger", },
    },
);

use Test::More qw(no_plan);
use lib "../App-Context/lib";
use lib "../../App-Context/lib";
use lib "lib";
use lib "../lib";

use App;
use App::Repository;
use strict;

my $context = App->context(
    conf_file => "",
    conf => {
        Repository => {
            default => {
                class => $App::options{dbclass},
                dbdriver => $App::options{dbdriver},
                dbhost => $App::options{dbhost},
                dbname => $App::options{dbname},
                dbuser => $App::options{dbuser},
                dbpass => $App::options{dbpass},
                table => {
                    test_person => {
                        primary_key => ["person_id"],
                    },
                },
            },
        },
        SessionObject => {
            adults => {
                class => "App::SessionObject::RepositoryObjectSet",
                #repository => "default",
                #table => "test_person",
                #params => {
                #    "age.ge" => 18,
                #},
            },
        },
    },
);

my $rep = $context->repository();

{
    #cheating... I know its a DBI, but I have to set up the test somehow
    my $dbh     = $rep->{dbh};
    eval { $dbh->do("drop table test_person"); };
    my $ddl     = <<EOF;
create table test_person (
    person_id          integer      not null auto_increment primary key,
    first_name         varchar(99)  null,
    last_name          varchar(99)  null,
    address            varchar(99)  null,
    city               varchar(99)  null,
    state              varchar(99)  null,
    zip                varchar(10)  null,
    country            char(2)      null,
    home_phone         varchar(99)  null,
    work_phone         varchar(99)  null,
    email_address      varchar(99)  null,
    gender             char(1)      null,
    birth_dt           date         null,
    age                integer      null,
    index person_ie1 (last_name, first_name)
)
EOF
    $dbh->do($ddl);
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (1,39,'stephen',  'M','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (2,37,'susan',    'F','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (3, 6,'maryalice','F','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (4, 3,'paul',     'M','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (5, 1,'christine','F','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (6,45,'tim',      'M','GA')");
    $dbh->do("insert into test_person (person_id,age,first_name,gender,state) values (7,39,'keith',    'M','GA')");
}

###########################################################################
# DATA ACCESS TESTS
###########################################################################
my ($person_id, $first_name, $last_name, $address, $city, $state, $zip, $country);
my ($home_phone, $work_phone, $email_address, $gender, $birth_dt, $age);

my $columns = [ "person_id", "age", "first_name", "gender", "state" ];
my $rows = [
    [ 1, 39, "stephen",   "M", "GA", ],
    [ 2, 37, "susan",     "F", "GA", ],
    [ 3,  6, "maryalice", "F", "GA", ],
    [ 4,  3, "paul",      "M", "GA", ],
    [ 5,  1, "christine", "F", "GA", ],
    [ 6, 45, "tim",       "M", "GA", ],
    [ 7, 39, "keith",     "M", "GA", ],
];

my ($row, $nrows);

#####################################################################
#  $value  = $rep->get ($table, $key,     $col,   \%options);
#  $rep->set($table, $key,     $col,   $value,    \%options);
#####################################################################
{
    my $objset = $context->session_object("adults");
    ok(1, "looks good");
    my ($objects, $index);
    eval {
        $objects = $objset->get_objects();
    };
    ok($@ =~ /table not defined/, "table not defined");
    $objset->set_table("test_person");
    $objects = $objset->get_objects();
    ok($#$objects == 6, "got all 7 objects");
    $objset->set_params({ "age.ge" => 18 });
    $objects = $objset->get_objects();
    ok($#$objects == 3, "got 4 objects");
    $objset->set_params({});
    $objects = $objset->get_objects("F",["gender"]);
    ok($#$objects == 2, "got 3 female objects");
    $objects = $objset->get_objects("M");
    ok($#$objects == 3, "got 4 male objects");
    $index = $objset->get_index();
    ok(ref($index) eq "HASH", "got a hashref for an index");
    ok(defined $index->{M}, "M part of index found");
    ok(defined $index->{F}, "F part of index found");
    ok(ref($index->{M}) eq "ARRAY", "M part of index ARRAY ref");
    ok(ref($index->{F}) eq "ARRAY", "F part of index ARRAY ref");
    my $values = $objset->get_column_values("gender");
    is_deeply($values, ["M","F"], "gender values");
    $index = $objset->get_unique_index("ak1", ["first_name"]);
    is($index->{stephen}{age}, 39, "get_unique_index worked on stephen");
    $objset->set_params({ "age.ge" => 1 });
    $objset->update_params({ "age.ge" => 18, first_name => "stephen"});
    $objects = $objset->get_objects();
    ok($#$objects == 3, "got 4 objects");
    $objset->get_unique_index(["first_name"]);
    my $object = $objset->get_object("stephen");
    ok($object->{age} == 39, "got stephen object (age 39)");
}

{
    my $dbh = $rep->{dbh};
    $dbh->do("drop table test_person");
}

exit 0; 
