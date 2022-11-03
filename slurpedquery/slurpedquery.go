package slurpedquery

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"reflect"
)

func typeIsRightJustified(candidate reflect.Type) bool {
	if candidate == nil {
		return false
	}
	rightJustifiedKinds := map[reflect.Kind]bool{
		reflect.Int:    true,
		reflect.Int8:   true,
		reflect.Int16:  true,
		reflect.Int32:  true,
		reflect.Int64:  true,
		reflect.Uint:   true,
		reflect.Uint8:  true,
		reflect.Uint16: true,
		reflect.Uint32: true,
		reflect.Uint64: true,
	}
	if _, ok := rightJustifiedKinds[candidate.Kind()]; ok {
		return true
	}
	return false
}

func typeIsDotJustified(candidate reflect.Type) bool {
	if candidate == nil {
		return false
	}
	dotJustifiedTypes := map[reflect.Kind]bool{
		reflect.Float32: true,
		reflect.Float64: true,
	}
	if _, ok := dotJustifiedTypes[candidate.Kind()]; ok {
		return true
	}
	return false
}

type SlurpedQuery struct {
	Headers    []string
	Types      []reflect.Type
	Widths     []int
	StringRows [][]string
	Rows       [][]interface{}
}

// Slurp reads a query result
func (sq *SlurpedQuery) Slurp(rows *sql.Rows) error {
	var err error

	// Set headers and initialize column widths.
	if sq.Headers, err = rows.Columns(); err != nil {
		return fmt.Errorf("sq headers: %w", err)
	}
	// TODO: check here if any header is nil, and set it to "(null)".
	// sqlite: "select 1 as null" will show this, postgres is ok
	sq.Types = make([]reflect.Type, len(sq.Headers))
	for _, h := range sq.Headers {
		sq.Widths = append(sq.Widths, len(h))
	}

	// Append each row of data and calculate widths.
	for rows.Next() {
		line := make([]interface{}, len(sq.Headers))
		linePtrs := make([]interface{}, len(sq.Headers))
		for i, _ := range sq.Headers {
			linePtrs[i] = &line[i]
		}
		if err = rows.Scan(linePtrs...); err != nil {
			return fmt.Errorf("sq scan: %w", err)
		}
		sq.Rows = append(sq.Rows, line)
		strline := make([]string, len(sq.Headers))
		for i, datum := range line {
			if len(sq.Rows) == 1 {
				sq.Types[i] = reflect.TypeOf(datum)
			}
			if datum == nil {
				datum = "(null)"
			}
			strline[i] = fmt.Sprintf("%v", datum)
			if len(strline[i]) > sq.Widths[i] {
				sq.Widths[i] = len(strline[i])
			}
		}
		sq.StringRows = append(sq.StringRows, strline)
	}
	return nil
}

// dashes generates a string of n dashes.
func dashes(n int) string {
	// use string builder?
	d := ""
	for i := 0; i < n; i++ {
		d = d + "-"
	}
	return d
}

const (
	noDecoration      = iota // "     "
	asciiDecoration          // "|-+++"
	unicodeDecoration        // "│─├┼┤"
)

func (sq *SlurpedQuery) PrettyPrint(output ...io.Writer) {
	if len(sq.Headers) == 0 {
		return
	}
	var f io.Writer
	if len(output) == 0 {
		f = os.Stdout
	} else {
		f = output[0]
	}

	// header
	fmt.Fprint(f, "|")
	for i := range sq.Headers {
		fmt.Fprintf(f, " %-*s |", sq.Widths[i], sq.Headers[i])
	}
	fmt.Fprintln(f)

	// divider
	fmt.Fprint(f, "+")
	for i := range sq.Headers {
		wid := sq.Widths[i]
		fmt.Fprintf(f, "-%*s-+", wid, dashes(wid))
	}
	fmt.Fprintln(f)

	// rows
	for _, r := range sq.Rows {
		fmt.Fprint(f, "|")
		for i, val := range r {
			var pval string
			if val == nil {
				pval = "(null)"
			} else {
				pval = fmt.Sprintf("%v", val)
			}
			if typeIsRightJustified(sq.Types[i]) {
				fmt.Fprintf(f, " %*v |", sq.Widths[i], pval)
				// TODO figure out dot justfied formatting
			} else if typeIsDotJustified(sq.Types[i]) {
				fmt.Fprintf(f, " %*v |", sq.Widths[i], pval)
			} else {
				fmt.Fprintf(f, " %-*v |", sq.Widths[i], pval)
			}
		}
		fmt.Fprintln(f)
	}
}
