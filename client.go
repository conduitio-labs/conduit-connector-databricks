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
	"errors"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/databricks/databricks-sql-go"
	"github.com/rs/zerolog"
)

func init() {
	// Databricks' driver uses the UNIX time in some cases
	// which is not compatible with what Conduit expects
	zerolog.TimeFieldFormat = time.RFC3339
}

const ansiMode = "ansi_mode"

type queryBuilder interface {
	buildInsert(table string, columns []string, values []interface{}, types []string) (string, error)

	describeTable(table string) string
}

type sqlClient struct {
	db           *sql.DB
	tableName    string
	columnTypes  map[string]string
	queryBuilder queryBuilder
}

func newClient() *sqlClient {
	return &sqlClient{
		columnTypes:  make(map[string]string),
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

func (c *sqlClient) Insert(ctx context.Context, record sdk.Record) error {
	sdk.Logger(ctx).Trace().Msg("inserting record")

	var colNames []string
	var values []interface{}
	var types []string

	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("error unmarshalling payload: %w", err)
	}

	key := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Key.Bytes(), &key); err != nil {
		return fmt.Errorf("error unmarshalling key: %w", err)
	}

	// merge key and payload fields
	// todo we can probably remove column types and simplify this
	for colName, colType := range c.columnTypes {
		value, ok := payload[colName]
		if !ok {
			value, ok = key[colName]
			if !ok {
				continue
			}
		}
		colNames = append(colNames, colName)
		values = append(values, value)
		types = append(types, colType)
	}

	sqlString, err := c.queryBuilder.buildInsert(c.tableName, colNames, values, types)
	if err != nil {
		return fmt.Errorf("failed building query: %w", err)
	}
	sdk.Logger(ctx).Trace().Msgf("sql string\n%v\n", sqlString)

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

func (c *sqlClient) Update(context.Context, sdk.Record) error {
	return errors.New("update not implemented")
}

func (c *sqlClient) Delete(context.Context, sdk.Record) error {
	return errors.New("delete not implemented")
}

// getColumnInfo gets information on all the column names and types and stores them
func (c *sqlClient) getColumnInfo() error {
	var ignoredValue sql.NullString

	rows, err := c.db.Query(c.queryBuilder.describeTable(c.tableName))
	if err != nil {
		return fmt.Errorf("failed to execute describe query: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName string
		var colType string
		err := rows.Scan(&colName, &colType, &ignoredValue)
		if err != nil {
			return fmt.Errorf("failed to next(): %v", err)
		}

		c.columnTypes[colName] = colType
	}

	return nil
}
