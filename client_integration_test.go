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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/matryer/is"
)

var errMissingConfig = errors.New("missing config value")

type testHelper struct {
	cfg Config
	db  *sql.DB
}

func newTestHelper() (*testHelper, error) {
	token := os.Getenv("DATABRICKS_API_TOKEN")
	if token == "" {
		return nil, fmt.Errorf("token: %w", errMissingConfig)
	}

	host := os.Getenv("DATABRICKS_HOST")
	if host == "" {
		return nil, fmt.Errorf("host: %w", errMissingConfig)
	}

	portStr := os.Getenv("DATABRICKS_PORT")
	if portStr == "" {
		return nil, fmt.Errorf("port: %w", errMissingConfig)
	}
	port, err := strconv.ParseInt(portStr, 10, 0)
	if err != nil {
		return nil, err
	}

	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	if httpPath == "" {
		return nil, fmt.Errorf("http path: %w", errMissingConfig)
	}

	cfg := Config{
		Token:     token,
		Host:      host,
		Port:      int(port),
		HTTPath:   httpPath,
		TableName: fmt.Sprintf("hive_metastore.default.test_table_%v", time.Now().UnixMilli()),
	}

	th := &testHelper{cfg: cfg}
	db, err := th.connect()
	if err != nil {
		return nil, fmt.Errorf("failed connectoring to DB: %w", err)
	}
	th.db = db

	err = th.createTestTable(cfg)
	if err != nil {
		return nil, fmt.Errorf("couldn't create test table: %w", err)
	}

	return th, nil
}

func (th *testHelper) createTestTable(cfg Config) error {
	testTableCreate := `CREATE TABLE %s (
    id int,
    name varchar(255),
    full_time boolean,
    updated_at timestamp
);`

	_, err := th.db.Exec(fmt.Sprintf(testTableCreate, cfg.TableName))
	if err != nil {
		return fmt.Errorf("failed creating test table %v: %w", cfg.TableName, err)
	}
	time.Sleep(5 * time.Second)
	return nil
}

func (th *testHelper) connect() (*sql.DB, error) {
	connector, err := dbsql.NewConnector(
		dbsql.WithAccessToken(th.cfg.Token),
		dbsql.WithServerHostname(th.cfg.Host),
		dbsql.WithPort(th.cfg.Port),
		dbsql.WithHTTPPath(th.cfg.HTTPath),
		dbsql.WithSessionParams(map[string]string{
			ansiMode: "true",
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return sql.OpenDB(connector), nil
}

func (th *testHelper) cleanup() error {
	_, err := th.db.Exec(fmt.Sprintf("DROP TABLE %v", th.cfg.TableName))
	return err
}

func TestSqlClient_Insert(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	wantID := 123
	wantName := "test name"
	wantFullTime := true
	wantUpdatedAt := time.Now().Truncate(time.Millisecond).UTC()

	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationCreate,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": wantID},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"name":       wantName,
				"full_time":  wantFullTime,
				"updated_at": wantUpdatedAt,
			},
		},
	}
	err = underTest.Insert(ctx, rec)
	is.NoErr(err)

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName) //nolint:gosec // ok since this is a test
	is.NoErr(err)

	count := 0
	for rows.Next() {
		var gotID int
		var gotName string
		var gotFullTime bool
		var gotUpdatedAt time.Time

		err := rows.Scan(&gotID, &gotName, &gotFullTime, &gotUpdatedAt)
		is.NoErr(err)

		count++
		is.Equal(wantID, gotID)
		is.Equal(wantName, gotName)
		is.Equal(wantFullTime, gotFullTime)
		is.Equal(wantUpdatedAt, gotUpdatedAt)
	}
	is.Equal(1, count)
}

func TestSqlClient_Insert_NonExistingColumn(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationCreate,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": 123},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"foobar": "foobar",
			},
		},
	}
	err = underTest.Insert(ctx, rec)
	is.True(err != nil)
	is.True(strings.Contains(
		err.Error(),
		"databricks: execution error: failed to execute query: [UNRESOLVED_COLUMN.WITH_SUGGESTION] "+
			"A column or function parameter with name `foobar` cannot be resolved.",
	))
}

func TestClient_Update_DoesntExist(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	// perform update
	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationUpdate,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": 123},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"name": "a name",
			},
		},
	}
	err = underTest.Update(ctx, rec)
	is.NoErr(err)

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName) //nolint:gosec // ok since this is a test
	is.NoErr(err)
	is.True(!rows.Next())
}

func TestClient_Update_Partial(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	wantID := 123
	wantName := "test name"
	wantFullTime := true
	wantUpdatedAt := time.Now().Truncate(time.Millisecond).UTC()

	// insert row
	q, _, err := dialect.Insert(th.cfg.TableName).
		Cols("id", "name", "full_time", "updated_at").
		Vals([]interface{}{123, "name should be updated", true, time.Now().Add(-time.Hour).Truncate(time.Millisecond).UTC()}).
		ToSQL()
	is.NoErr(err)
	result, err := th.db.ExecContext(ctx, q)
	is.NoErr(err)
	affected, err := result.RowsAffected()
	is.NoErr(err)
	is.Equal(int64(1), affected)

	// perform update
	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationUpdate,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": wantID},
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"name": wantName,
				// full_time left out
				"updated_at": wantUpdatedAt,
			},
		},
	}
	err = underTest.Update(ctx, rec)
	is.NoErr(err)

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName) //nolint:gosec // ok since this is a test
	is.NoErr(err)

	count := 0
	for rows.Next() {
		var gotID int
		var gotName string
		var gotFullTime bool
		var gotUpdatedAt time.Time

		err := rows.Scan(&gotID, &gotName, &gotFullTime, &gotUpdatedAt)
		is.NoErr(err)

		count++
		is.Equal(wantID, gotID)
		is.Equal(wantName, gotName)
		is.Equal(wantFullTime, gotFullTime)
		is.Equal(wantUpdatedAt, gotUpdatedAt)
	}
	is.Equal(1, count)
}

func TestClient_Delete_Exists(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	// insert row
	id := 123
	q, _, err := dialect.Insert(th.cfg.TableName).
		Cols("id", "name", "full_time", "updated_at").
		Vals([]interface{}{id, "bye bye", true, time.Now().Add(-time.Hour).Truncate(time.Millisecond).UTC()}).
		ToSQL()
	is.NoErr(err)
	result, err := th.db.ExecContext(ctx, q)
	is.NoErr(err)
	affected, err := result.RowsAffected()
	is.NoErr(err)
	is.Equal(int64(1), affected)

	// perform update
	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationDelete,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": id},
	}
	err = underTest.Delete(ctx, rec)
	is.NoErr(err)

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName) //nolint:gosec // ok since this is a test
	is.NoErr(err)

	is.True(!rows.Next())
}

func TestClient_Delete_DoesntExist(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	underTest := newClient()
	th, err := newTestHelper()
	if errors.Is(err, errMissingConfig) {
		t.Skipf("configuration not provided")
	}
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	// perform update
	rec := sdk.Record{
		Position:  sdk.Position("test-pos"),
		Operation: sdk.OperationDelete,
		Metadata:  nil,
		Key:       sdk.StructuredData{"id": 123},
	}
	err = underTest.Delete(ctx, rec)
	is.NoErr(err)

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName) //nolint:gosec // ok since this is a test
	is.NoErr(err)

	is.True(!rows.Next())
}
