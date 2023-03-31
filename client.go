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
	db             *sql.DB
	TableName      string
	AllColumnInfos []map[string]ColumnInfo
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
	c.TableName = config.TableName
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

	var keys []string
	var values []interface{}
	var types []string

	var err error
	c.AllColumnInfos, err = c.readPayload(c.AllColumnInfos, record)
	if err != nil {
		return err
	}

	for _, col := range c.AllColumnInfos {
		for key, info := range col {
			keys = append(keys, key)
			values = append(values, info.value)
			types = append(types, info.dataType)
		}
	}

	//prepare SQL statement
	statement, err := c.PrepareSQL(ctx, keys, values)
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
	defer func(stmt *sql.Stmt) {
		err := stmt.Close()
		if err != nil {

		}
	}(stmt)

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

	describeQuery := "DESCRIBE " + c.TableName

	rows, err := c.db.Query(describeQuery)
	if err != nil {
		return fmt.Errorf("failed to execute describe query: %v", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {

		}
	}(rows)

	for rows.Next() {
		newCol := map[string]ColumnInfo{}
		var colName string
		var colType string
		err := rows.Scan(&colName, &colType, &ignoredValue)
		if err != nil {
			return fmt.Errorf("failed to next(): %v", err)
		}
		newCol[colName] = ColumnInfo{
			dataType: colType,
			value:    nil,
		}

		c.AllColumnInfos = append(c.AllColumnInfos, newCol)
	}

	return nil
}

func (c *sqlClient) readPayload(AllColumns []map[string]ColumnInfo, record sdk.Record) ([]map[string]ColumnInfo, error) {
	payload := make(sdk.StructuredData)
	if err := json.Unmarshal(record.Payload.After.Bytes(), &payload); err != nil {
		return nil, fmt.Errorf("error unmarshalling: %w", err)
	}

	for _, columnInfoMap := range AllColumns {
		for key, columnInfo := range columnInfoMap {
			if value, ok := payload[key]; ok {
				columnInfo.value = value
				columnInfoMap[key] = columnInfo
			}
		}
	}

	return AllColumns, nil
}

func (c *sqlClient) PrepareSQL(ctx context.Context, keys []string, values []interface{}) (string, error) {

	sqlString, _, err := squirrel.
		Insert(c.TableName).
		Columns(keys...).
		Values(values...).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("error creating sqlString: %w", err)
	}

	sdk.Logger(ctx).Info().Msgf(" \n 0000000%v\n", sqlString)

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
