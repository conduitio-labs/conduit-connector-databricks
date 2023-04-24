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
	"fmt"
	dbsql "github.com/databricks/databricks-sql-go"
	"os"
	"strconv"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/matryer/is"
)

type testHelper struct {
	cfg Config
	db  *sql.DB
}

func newTestHelper() (*testHelper, error) {
	token := os.Getenv("DATABRICKS_API_TOKEN")
	host := os.Getenv("DATABRICKS_HOST")
	portStr := os.Getenv("DATABRICKS_PORT")
	port, _ := strconv.ParseInt(portStr, 10, 0)
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")

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
	is.NoErr(err)
	defer func() {
		is.NoErr(th.cleanup())
	}()

	err = underTest.Open(ctx, th.cfg)
	is.NoErr(err)

	wantID := 123
	wantName := "test name"
	wantFullTime := true
	wantUpdatedAt := time.Now().UTC()

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

	rows, err := th.db.Query("SELECT * FROM " + th.cfg.TableName)
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
