#!/usr/local/bin/perl -w

use Test::More qw(no_plan);
use lib "../App-Context/lib";
use lib "../../App-Context/lib";
use lib "lib";
use lib "../lib";

use_ok("App");
use_ok("App::Repository");
use strict;

my $context = App->context(
    conf_file => "",
    conf => {
        Repository => {
            default => {
                class => "App::Repository::MySQL",
                dbidriver => "mysql",
                dbhost => "frento",
                dbname => "test",
                dbuser => "dbuser",
                dbpass => "dbuser7",
                table => {
                    test_person => {
                        primary_key => ["person_id"],
                    },
                },
            },
        },
    },
);

my $db = $context->repository();

$App::trace_subs = 0;
$App::trace_subs = 0;

{
    #cheating... I know its a DBI, but I have to set up the test somehow
    my $dbh     = $db->{dbh};
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
    $db->_load_rep_metadata();
}

{
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [1,39,"stephen",  "M","GA"]),
        "insert row (primary key included)");
    ok($db->_insert_row("test_person", ["age","first_name","gender","state"],
        [37,"susan",    "F","GA"]),
        "insert row (primary key excluded, auto_increment)");
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [undef, 6,"maryalice","F","GA"]),
        "insert row (primary key included, null)");
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [0, 3,"paul",     "M","GA"]),
        "insert row (primary key included, 0)");
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [5, 1,"christine","F","GA"]),
        "insert again");
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [6,45,"tim",      "M","GA"]),
        "insert again");
    ok($db->_insert_row("test_person", ["person_id","age","first_name","gender","state"],
        [7,39,"keith",    "M","GA"]),
        "insert again");
    ok($db->insert("test_person", {
            person_id => 8,
            age => 35,
            first_name => "alex",
            gender => "M",
            state => "GA",
        }),
        "insert hash");
    eval {
        $db->insert_row("test_person", {
            person_id => 8,
            age => 35,
            first_name => "alex",
            gender => "M",
            state => "GA",
        });
    };
    ok($@, "insert dup hash fails");
    ok($db->insert("test_person", undef, {
            person_id => 9,
            age => 35,
            first_name => "alex",
            gender => "M",
            state => "GA",
        }),
        "insert hash in 2nd pos");
    ok($db->insert("test_person", ["age","first_name","gender","state"], {
            person_id => 9,
            age => 35,
            first_name => "alex",
            gender => "M",
            state => "GA",
        }),
        "insert hash in 2nd pos w/ col spec");
    eval {
        $db->insert_row("test_person", undef, {
            person_id => 9,
            age => 35,
            first_name => "alex",
            gender => "M",
            state => "GA",
        });
    };
    ok($@, "insert dup hash in 2nd pos fails");
}

exit 0;

