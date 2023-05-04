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
	"testing"

	"github.com/matryer/is"
)

func TestQueryBuilder_Insert(t *testing.T) {
	testCases := []struct {
		name string

		table   string
		columns []string
		values  []interface{}

		want    string
		wantErr string
	}{
		{
			name:    "not enough columns",
			table:   "products",
			columns: []string{"id"},
			values:  []interface{}{1, "computer"},
			want:    "",
			wantErr: "expected equal number of columns and values, but got 1 column(s) and 2 value(s)",
		},
		{
			name:    "not enough values",
			table:   "products",
			columns: []string{"id", "name"},
			values:  []interface{}{1},
			want:    "",
			wantErr: "expected equal number of columns and values, but got 2 column(s) and 1 value(s)",
		},
		{
			name:    "no table",
			table:   "",
			columns: []string{"id", "name"},
			values:  []interface{}{1, "computer"},
			want:    ``,
			wantErr: "error creating sqlString: insert statements must specify a table",
		},
		{
			name:    "simple insert",
			table:   "test.products",
			columns: []string{"id", "name"},
			values:  []interface{}{1, "computer"},
			want:    "INSERT INTO `test`.`products` (`id`, `name`) VALUES (1, 'computer')",
			wantErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest := &ansiQueryBuilder{}
			sql, err := underTest.buildInsert(tc.table, tc.columns, tc.values)
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

func TestQueryBuilder_Update(t *testing.T) {
	testCases := []struct {
		name string

		table  string
		keys   map[string]interface{}
		values map[string]interface{}

		want    string
		wantErr string
	}{
		{
			name:    "simple update",
			table:   "test.products",
			keys:    map[string]interface{}{"id": "a1b2"},
			values:  map[string]interface{}{"name": "strawberry yoghurt"},
			want:    "UPDATE `test`.`products` SET `name`='strawberry yoghurt' WHERE (`id` = 'a1b2')",
			wantErr: "",
		},
		{
			name:    "nil keys",
			table:   "test.products",
			keys:    nil,
			values:  map[string]interface{}{"name": "strawberry yoghurt"},
			want:    "",
			wantErr: "no keys provided",
		},
		{
			name:    "empty keys map",
			table:   "test.products",
			keys:    map[string]interface{}{},
			values:  map[string]interface{}{"name": "strawberry yoghurt"},
			want:    "",
			wantErr: "no keys provided",
		},
		{
			name:    "nil values",
			table:   "test.products",
			keys:    map[string]interface{}{"id": "a1b2"},
			values:  nil,
			want:    "",
			wantErr: "no values provided",
		},
		{
			name:    "empty values map",
			table:   "test.products",
			keys:    map[string]interface{}{"id": "a1b2"},
			values:  map[string]interface{}{},
			want:    "",
			wantErr: "no values provided",
		},
		{
			name:    "no table",
			table:   "",
			keys:    map[string]interface{}{"a": "b"},
			values:  map[string]interface{}{"c": "d"},
			want:    "",
			wantErr: "table name not provided",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest := &ansiQueryBuilder{}
			sql, err := underTest.buildUpdate(tc.table, tc.keys, tc.values)
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

func TestQueryBuilder_Delete(t *testing.T) {
	testCases := []struct {
		name string

		table string
		keys  map[string]interface{}

		want    string
		wantErr string
	}{
		{
			name:    "simple delete",
			table:   "test.products",
			keys:    map[string]interface{}{"id": "a1b2"},
			want:    "DELETE FROM `test`.`products` WHERE (`id` = 'a1b2')",
			wantErr: "",
		},
		{
			name:    "nil keys",
			table:   "test.products",
			keys:    nil,
			want:    "",
			wantErr: "no keys provided",
		},
		{
			name:    "empty keys map",
			table:   "test.products",
			keys:    map[string]interface{}{},
			want:    "",
			wantErr: "no keys provided",
		},
		{
			name:    "no table",
			table:   "",
			keys:    map[string]interface{}{"a": "b"},
			want:    "",
			wantErr: "table name not provided",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)

			underTest := &ansiQueryBuilder{}
			sql, err := underTest.buildDelete(tc.table, tc.keys)
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
