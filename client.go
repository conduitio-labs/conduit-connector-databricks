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
	"encoding/json"
	"fmt"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/rs/zerolog"
)

func init() {
	// Databricks' driver uses the UNIX time in some cases
	// which is not compatible with what Conduit expects
	zerolog.TimeFieldFormat = time.RFC3339
}

const ansiMode = "ansi_mode"

type queryBuilder interface {
	buildInsert(table string, values map[string]interface{}) (string, error)
	buildUpdate(table string, keys map[string]interface{}, values map[string]interface{}) (string, error)
	buildDelete(table string, keys map[string]interface{}) (string, error)

	describeTable(table string) string
}

type sqlClient struct {
	db           *sql.DB
	tableName    string
	columns      []string
	queryBuilder queryBuilder
}

func newClient() *sqlClient {
	return &sqlClient{
		queryBuilder: &ansiQueryBuilder{},
	}
}

func (c *sqlClient) Open(ctx context.Context, config Config) error {
	sdk.Logger(ctx).Debug().Msg("opening sql client")

	connector, err := dbsql.NewConnector(
		dbsql.WithAccessToken(config.Token),
		dbsql.WithServerHostname(config.Host),
		dbsql.WithPort(config.Port),
		dbsql.WithHTTPPath(config.HTTPath),
		dbsql.WithSessionParams(map[string]string{
			ansiMode: "true",
		}),
	)
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	db := sql.OpenDB(connector)

	sdk.Logger(ctx).Debug().Msg("pinging database")
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	c.db = db
	c.tableName = config.TableName

	err = c.getColumnInfo()
	if err != nil {
		return fmt.Errorf("unable to get column information: %w", err)
	}

	sdk.Logger(ctx).Debug().Msg("sql client opened")
	return nil
}

func (c *sqlClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

func (c *sqlClient) Insert(ctx context.Context, record opencdc.Record) error {
	sdk.Logger(ctx).Trace().Msg("inserting record")

	payload := make(opencdc.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("error unmarshalling payload: %w", err)
	}

	key := make(opencdc.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &key); err != nil {
		return fmt.Errorf("error unmarshalling key: %w", err)
	}

	insertValues := c.merge(payload, key)

	sqlString, err := c.queryBuilder.buildInsert(c.tableName, insertValues)
	if err != nil {
		return fmt.Errorf("failed building query: %w", err)
	}
	sdk.Logger(ctx).Trace().Msgf("insert sql string\n%v\n", sqlString)

	// Currently, Databricks doesn't support prepared statements
	// sqlString here comes with all the values filled in.
	// However, it looks like Databricks is close to supporting it:
	// https://github.com/databricks/databricks-sql-go/issues/84#issuecomment-1516815045
	stmt, err := c.db.Prepare(sqlString)
	if err != nil {
		return fmt.Errorf("failed to prepare db statement: %w", err)
	}
	defer stmt.Close()

	res, err := stmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute db statement: %w ", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get number of affected rows: %w ", err)
	}
	if affected != 1 {
		return fmt.Errorf("%v rows inserted", affected)
	}

	return nil
}

func (c *sqlClient) Update(ctx context.Context, record opencdc.Record) error {
	sdk.Logger(ctx).Trace().Msg("updating record")

	// nothing to update
	if record.Payload.After == nil || len(record.Payload.After.Bytes()) == 0 {
		return nil
	}

	payload := make(opencdc.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("error unmarshalling payload: %w", err)
	}

	key := make(opencdc.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &key); err != nil {
		return fmt.Errorf("error unmarshalling key: %w", err)
	}

	sqlString, err := c.queryBuilder.buildUpdate(c.tableName, key, payload)
	if err != nil {
		return fmt.Errorf("failed building update query: %w", err)
	}
	sdk.Logger(ctx).Trace().Msgf("update sql string\n%v\n", sqlString)

	// we're not checking the number of affected rows
	// as we're not even sure that a row with the same key has already been inserted
	_, err = c.db.ExecContext(ctx, sqlString)
	if err != nil {
		return fmt.Errorf("failed update: %w", err)
	}

	return nil
}

func (c *sqlClient) Delete(ctx context.Context, record opencdc.Record) error {
	sdk.Logger(ctx).Trace().Msg("deleting record")

	key := make(opencdc.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &key); err != nil {
		return fmt.Errorf("error unmarshalling key: %w", err)
	}

	sqlString, err := c.queryBuilder.buildDelete(c.tableName, key)
	if err != nil {
		return fmt.Errorf("failed building delete query: %w", err)
	}
	sdk.Logger(ctx).Trace().Msgf("delete sql string\n%v\n", sqlString)

	// we're not checking the number of affected rows
	// as we're not even sure that a row with the same key has already been inserted
	_, err = c.db.ExecContext(ctx, sqlString)
	if err != nil {
		return fmt.Errorf("failed delete: %w", err)
	}

	return nil
}

// getColumnInfo gets information on all the column names and types and stores them
func (c *sqlClient) getColumnInfo() error {
	// we'll ignore the comment
	var ignore sql.NullString

	rows, err := c.db.Query(c.queryBuilder.describeTable(c.tableName))
	if err != nil {
		return fmt.Errorf("failed to execute describe query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName string
		err := rows.Scan(&colName, &ignore, &ignore)
		if err != nil {
			return fmt.Errorf("failed to next(): %v", err)
		}

		c.columns = append(c.columns, colName)
	}

	return nil
}

func (c *sqlClient) merge(m1, m2 map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for k, v := range m1 {
		merged[k] = v
	}
	for k, v := range m2 {
		merged[k] = v
	}

	return merged
}
