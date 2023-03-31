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
	"github.com/Masterminds/squirrel"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	mysql "github.com/databricks/databricks-sql-go"
	"github.com/rs/zerolog"
)

func init() {
	// Databricks' driver uses the UNIX time in some cases
	// which is not compatible with what Conduit expects
	zerolog.TimeFieldFormat = time.RFC3339
}

const ansiMode = "ansi_mode"

type sqlClient struct {
	db          *sql.DB
	tableName   string
	columnTypes map[string]string
}

type ColumnInfo struct {
	dataType string
	value    interface{}
}

func newClient() *sqlClient {
	return &sqlClient{}
}

func (c *sqlClient) Open(ctx context.Context, config Config) error {
	sdk.Logger(ctx).Debug().Msg("opening sql client")

	connector, err := mysql.NewConnector(
		mysql.WithAccessToken(config.Token),
		mysql.WithServerHostname(config.Host),
		mysql.WithPort(config.Port),
		mysql.WithHTTPPath(config.HTTPath),
		mysql.WithSessionParams(map[string]string{
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
	if err != nil {
		return fmt.Errorf("failed to get field count: %w", err)
	}

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
	sdk.Logger(ctx).Info().Msg("inserting record")

	var colNames []string
	var values []interface{}
	var types []string

	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return fmt.Errorf("error unmarshalling: %w", err)
	}

	for colName, colType := range c.columnTypes {
		colNames = append(colNames, colName)
		values = append(values, payload[colName])
		types = append(types, colType)
	}

	//prepare SQL statement
	statement, err := c.PrepareSQL(ctx, colNames, values)
	if err != nil {
		return err
	}

	//build SQL statement
	statement, err = c.BuildSQL(statement, values, types)
	if err != nil {
		return err
	}

	stmt, err := c.db.Prepare(statement)
	if err != nil {
		return fmt.Errorf("failed to prepare db statement: %w", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("failed to execute db statement: %w ", err)
	}

	return nil
}

func (c *sqlClient) Update(context.Context, sdk.Record) error {

	return nil
}

func (c *sqlClient) Delete(ctx context.Context, record sdk.Record) error {

	return nil
}

func (c *sqlClient) Snapshot(ctx context.Context, record sdk.Record) error {

	return nil
}

// getColumnInfo gets information on all the column names and types and stores them
func (c *sqlClient) getColumnInfo() error {

	var ignoredValue sql.NullString

	describeQuery := "DESCRIBE " + c.tableName

	rows, err := c.db.Query(describeQuery)
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

func (c *sqlClient) PrepareSQL(ctx context.Context, keys []string, values []interface{}) (string, error) {

	sqlString, _, err := squirrel.
		Insert(c.tableName).
		Columns(keys...).
		Values(values...).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("error creating sqlString: %w", err)
	}

	sdk.Logger(ctx).Trace().Msgf("sql string\n%v\n", sqlString)

	return sqlString, nil
}

func (c *sqlClient) BuildSQL(sql string, values []interface{}, types []string) (string, error) {

	if len(values) != len(types) {
		return "", fmt.Errorf("values and types slices should have the same length")
	}

	formattedValues := make([]string, len(values))
	for i, value := range values {
		formattedValues[i] = fmt.Sprintf("try_cast(\"%v\" as %s)", value, types[i])
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
