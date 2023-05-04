// Copyright © 2023 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package databricks

import (
	"errors"
	"fmt"
	"strings"

	"github.com/doug-martin/goqu/v9"
)

func init() {
	opts := goqu.DefaultDialectOptions()
	// Databricks identifiers are enclosed in backticks
	// https://docs.databricks.com/sql/language-manual/sql-ref-identifiers.html
	opts.QuoteRune = '`'
	goqu.RegisterDialect("databricks-dialect", opts)
}

var dialect = goqu.Dialect("databricks-dialect")

type ansiQueryBuilder struct {
}

func (b *ansiQueryBuilder) buildDelete(
	table string,
	keys map[string]interface{},
) (string, error) {
	if table == "" {
		return "", errors.New("table name not provided")
	}
	if len(keys) == 0 {
		return "", errors.New("no keys provided")
	}

	// transforms keys map into a goqu.Ex
	w := goqu.Ex{}
	for k, v := range keys {
		w[k] = v
	}
	q, _, err := dialect.Delete(table).
		Where(w).
		ToSQL()

	return q, err
}

func (b *ansiQueryBuilder) buildUpdate(
	table string,
	keys map[string]interface{},
	values map[string]interface{},
) (string, error) {
	if table == "" {
		return "", errors.New("table name not provided")
	}
	if len(keys) == 0 {
		return "", errors.New("no keys provided")
	}
	if len(values) == 0 {
		return "", errors.New("no values provided")
	}

	// transforms keys map into a goqu.Ex
	w := goqu.Ex{}
	for k, v := range keys {
		w[k] = v
	}
	q, _, err := dialect.Update(table).
		Set(values).
		Where(w).
		ToSQL()

	return q, err
}

// buildInsert builds an insert query.
func (b *ansiQueryBuilder) buildInsert(
	table string,
	columns []string,
	values []interface{},
) (string, error) {
	// Prepare SQL statement
	if len(columns) != len(values) {
		return "", fmt.Errorf(
			"expected equal number of columns and values, but got %v column(s) and %v value(s)",
			len(columns),
			len(values),
		)
	}
	if strings.TrimSpace(table) == "" {
		return "", errors.New("error creating sqlString: insert statements must specify a table")
	}

	var cols []interface{}
	for _, col := range columns {
		cols = append(cols, col)
	}
	q, _, err := dialect.Insert(table).
		Cols(cols...).
		Vals(values).
		ToSQL()

	return q, err
}

func (b *ansiQueryBuilder) describeTable(table string) string {
	return "DESCRIBE " + table
}
