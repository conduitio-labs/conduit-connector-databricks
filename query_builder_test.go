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
	"github.com/matryer/is"
	"testing"
)

func TestQueryBuilder(t *testing.T) {
	testCases := []struct {
		name string

		table   string
		columns []string
		values  []interface{}
		types   []string

		want    string
		wantErr string
	}{
		{
			name:    "not enough columns",
			table:   "products",
			columns: []string{"id"},
			values:  []interface{}{1, "computer"},
			types:   []string{"int", "varchar(100)"},
			want:    "",
			wantErr: "expected equal number of columns and values, but got 1 column(s) and 2 value(s)",
		},
		{
			name:    "not enough values",
			table:   "products",
			columns: []string{"id", "name"},
			values:  []interface{}{1},
			types:   []string{"int", "varchar(100)"},
			want:    "",
			wantErr: "expected equal number of columns and values, but got 2 column(s) and 1 value(s)",
		},
		{
			name:    "not enough types",
			table:   "products",
			columns: []string{"id", "name"},
			values:  []interface{}{1, "computer"},
			types:   []string{"int"},
			want:    "",
			wantErr: "expected equal number of columns and values, but got 2 value(s) and 1 type(s)",
		},
		{
			name:    "no table",
			table:   "",
			columns: []string{"id", "name"},
			values:  []interface{}{1, "computer"},
			types:   []string{"int", "varchar(100)"},
			want:    ``,
			wantErr: "error creating sqlString: insert statements must specify a table",
		},
		{
			name:    "simple insert",
			table:   "products",
			columns: []string{"id", "name"},
			values:  []interface{}{1, "computer"},
			types:   []string{"int", "varchar(100)"},
			want:    `INSERT INTO products (id,name) VALUES (cast("1" as int),cast("computer" as varchar(100)))`,
			wantErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest := &ansiQueryBuilder{}
			sql, err := underTest.buildInsert(tc.table, tc.columns, tc.values, tc.types)
			if tc.wantErr != "" {
				is.Equal("", sql)
				is.Equal(tc.wantErr, err.Error())

				return
			}

			is.NoErr(err)
			is.Equal(tc.want, sql)
		})
	}
}