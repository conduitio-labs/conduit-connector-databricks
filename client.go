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
	"time"

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

type sqlClient struct {
	db         *sql.DB
	FieldCount int
}

func newClient() *sqlClient {
	return &sqlClient{}
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
	c.FieldCount, err = c.GetFieldCount(config)
	if err != nil {
		return fmt.Errorf("failed to get field count: %w", err)
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

func (c *sqlClient) HandleRecord(ctx context.Context, record sdk.Record) error {

	//determine the operation
	err := sdk.Util.Destination.Route(
		ctx,
		record,
		c.Insert,
		c.Update,
		c.Delete,
		c.Snapshot,
	)
	if err != nil {
		return fmt.Errorf("failed to route operation: %w", err)
	}

	return nil
}

func (c *sqlClient) Insert(ctx context.Context, record sdk.Record) error {
	//payload := make(sdk.StructuredData)
	return nil
}

func (c *sqlClient) Update(ctx context.Context, record sdk.Record) error {

	return nil
}

func (c *sqlClient) Delete(ctx context.Context, record sdk.Record) error {

	return nil
}

func (c *sqlClient) Snapshot(ctx context.Context, record sdk.Record) error {

	return nil
}

func (c *sqlClient) PrepareSQL() (string, error) {

	return "", nil
}

func (c *sqlClient) GetFieldCount(config Config) (int, error) {

	columns, err := c.db.Query("SHOW COLUMNS FROM " + config.TableName)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}

	//Iterate through row object to count number of columns
	count := 0
	for columns.Next() {
		count++
	}

	return count, nil
}
