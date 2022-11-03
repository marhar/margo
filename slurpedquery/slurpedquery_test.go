package slurpedquery

import (
	"bytes"
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/lib/pq"
	"github.com/marhar/textwrap"
	_ "github.com/mattn/go-sqlite3"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func connect(which string) *sql.DB {
	var db *sql.DB
	var err error
	switch which {
	case "postgres":
		db, err = sql.Open("postgres", "postgresql://postgres@?sslmode=disable")
	case "pgx":
		db, err = sql.Open("pgx", "postgresql://postgres@?sslmode=disable")
	case "sqlite":
		db, err = sql.Open("sqlite3", "/tmp/junk.db")
	default:
		panic("must specify postgres, pgx or sqlite")
	}
	check(err)
	err = db.Ping()
	check(err)
	return db
}

func t7(driverName string, db *sql.DB) {
	prep(db, driverName)
	for _, qstring := range []string{
		"select * from t where a is not null order by a",
		"select * from t where a is null order by a",
		"select * from t where b=11 order by a",
		"select * from t where b <>5555 order by a",
		"select * from t order by a",
		"select a as \"this is very long as fa sdf dfa fafadfad  fad f\" from t order by a",
		"select 1,2,3,4,5,6",
		"select null,null,null",
		"select null x ,null,null yyyyyyyy",
		"select 1 as \"null\"",
		// TODO: fix for sqlite: "select 1 as null", pg treats it as column named "null"
	} {
		rows, err := db.Query(qstring)
		check(err)
		defer rows.Close()
		sq := new(SlurpedQuery)
		sq.Slurp(rows)
		fmt.Println()
		fmt.Println(qstring)
		//fmt.Printf("%+v\n", *sq)
		//pp.Print(*sq)
		sq.PrettyPrint()
	}
}

func runTest(driverName string) {
	db := connect(driverName)
	defer db.Close()
	t7(driverName, db)
}

func prep(db *sql.DB, driverName string) {
	var err error
	_, _ = db.Exec("drop table t")
	_, err = db.Exec("create table t(a text, b int, c float, driver text)")
	check(err)
	_, err = db.Exec(`
        insert into t(a,b,c,driver) values
          ('aaa',11,22.22,$1),
          (null,null,null,null),
          ('bbb',33,44.44,$2)
	   `, driverName, driverName)
	check(err)
}

func tpg(db *sql.DB, t *testing.T, qstring string, expected string) {
	rows, err := db.Query(qstring)
	check(err)
	defer rows.Close()
	sq := new(SlurpedQuery)
	sq.Slurp(rows)
	// TODO: test sq content

	var b bytes.Buffer
	sq.PrettyPrint(&b)
	actual := b.String()
	expected = textwrap.Dedent(expected)
	if actual != expected {
		fmt.Println(actual)
		fmt.Println(expected)
		t.Errorf("actual <%s>, expected <%s>", actual, expected)
	}
}

func TestPostgres(t *testing.T) {
	xname := "postgres"
	db := connect(xname)
	defer db.Close()
	prep(db, xname)
	tpg(db, t,
		"select * from t where a is not null order by a", `
         | a   | b  | c     | driver   |
         +-----+----+-------+----------+
         | aaa | 11 | 22.22 | postgres |
         | bbb | 33 | 44.44 | postgres |
		`)
	tpg(db, t,
		"select * from t where a is not null order by a", `
         | a   | b  | c     | driver   |
         +-----+----+-------+----------+
         | aaa | 11 | 22.22 | postgres |
         | bbb | 33 | 44.44 | postgres |
		`)
	tpg(db, t,
		"select * from t where a is not null order by a", `
         | a   | b  | c     | driver   |
         +-----+----+-------+----------+
         | aaa | 11 | 22.22 | postgres |
         | bbb | 33 | 44.44 | postgres |
		`)
	tpg(db, t,
		"select * from t where a is null order by a", `
         | a      | b      | c      | driver |
         +--------+--------+--------+--------+
         | (null) | (null) | (null) | (null) |
		`)
	tpg(db, t,
		"select * from t where b=11 order by a", `
         | a   | b  | c     | driver   |
         +-----+----+-------+----------+
         | aaa | 11 | 22.22 | postgres |
		`)
	tpg(db, t,
		"select * from t where b <>5555 order by a", `
         | a   | b  | c     | driver   |
         +-----+----+-------+----------+
         | aaa | 11 | 22.22 | postgres |
         | bbb | 33 | 44.44 | postgres |
		`)
	tpg(db, t,
		"select * from t order by a", `
         | a      | b      | c      | driver   |
         +--------+--------+--------+----------+
         | aaa    |     11 |  22.22 | postgres |
         | bbb    |     33 |  44.44 | postgres |
         | (null) | (null) | (null) | (null)   |
		`)
	tpg(db, t,
		"select a as \"very long column heading\" from t order by a", `
         | very long column heading |
         +--------------------------+
         | aaa                      |
         | bbb                      |
         | (null)                   |
		`)
	tpg(db, t,
		"select 1,2,3,4,5,6", `
         | ?column? | ?column? | ?column? | ?column? | ?column? | ?column? |
         +----------+----------+----------+----------+----------+----------+
         |        1 |        2 |        3 |        4 |        5 |        6 |
		`)
	tpg(db, t,
		"select null,null,null", `
         | ?column? | ?column? | ?column? |
         +----------+----------+----------+
         | (null)   | (null)   | (null)   |
		`)
	tpg(db, t,
		"select null x ,null,null yyyyyyyy", `
         | x      | ?column? | yyyyyyyy |
         +--------+----------+----------+
         | (null) | (null)   | (null)   |
		`)
	tpg(db, t,
		"select 1 as \"null\"", `
         | null |
         +------+
         |    1 |
		`)
}

/*
func TestAll(t *testing.T) {
	runTest("postgres")
	runTest("pgx")
	runTest("sqlite")
}
*/
