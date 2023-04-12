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
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
)

type ansiQueryBuilder struct {
}

func (b *ansiQueryBuilder) buildInsert(
	table string,
	columns []string,
	values []interface{},
	types []string,
) (string, error) {
	//prepare SQL statement
	sqlString, err := b.buildWithPlaceholders(table, columns, values)
	if err != nil {
		return "", err
	}

	//build SQL statement
	return b.fillPlaceholders(sqlString, values, types)
}

func (b *ansiQueryBuilder) buildWithPlaceholders(table string, columns []string, values []interface{}) (string, error) {
	if len(columns) != len(values) {
		return "", fmt.Errorf(
			"expected equal number of columns and values, but got %v column(s) and %v value(s)",
			len(columns),
			len(values),
		)
	}
	sqlString, _, err := squirrel.
		Insert(table).
		Columns(columns...).
		Values(values...).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("error creating sqlString: %w", err)
	}

	return sqlString, nil
}

func (b *ansiQueryBuilder) fillPlaceholders(sql string, values []interface{}, types []string) (string, error) {
	if len(values) != len(types) {
		return "", fmt.Errorf(
			"expected equal number of columns and values, but got %v value(s) and %v type(s)",
			len(values),
			len(types),
		)
	}

	formattedValues := make([]string, len(values))
	for i, value := range values {
		// todo make using cast an exception
		// e.g. when we get an int, and the column type is int, no need to cast
		formattedValues[i] = fmt.Sprintf("cast(\"%v\" as %s)", value, types[i])
	}

	placeholders := strings.Count(sql, "?")
	if placeholders != len(values) {
		return "", fmt.Errorf("number of placeholders in sql string should match the number of values")
	}

	sqlParts := strings.SplitN(sql, "?", placeholders+1)
	rewrittenSQL := strings.Builder{}

	for i, sqlPart := range sqlParts[:placeholders] {
		rewrittenSQL.WriteString(sqlPart)
		rewrittenSQL.WriteString(formattedValues[i])
	}

	rewrittenSQL.WriteString(sqlParts[placeholders])

	return rewrittenSQL.String(), nil
}

func (b *ansiQueryBuilder) describeTable(table string) string {
	return "DESCRIBE " + table
}
