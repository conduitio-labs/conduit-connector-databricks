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
	sdk "github.com/conduitio/conduit-connector-sdk"

	_ "github.com/databricks/databricks-sql-go"
)

type sqlClient struct {
	db *sql.DB
}

func newClient() *sqlClient {
	return &sqlClient{}
}

func (c sqlClient) Open(ctx context.Context, dsn string) error {
	sdk.Logger(ctx).Debug().Msg("opening sql client")

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		return fmt.Errorf("cannot open database: %w", err)
	}

	sdk.Logger(ctx).Debug().Msg("pinging database")
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	c.db = db

	sdk.Logger(ctx).Debug().Msg("sql client opened")
	return nil
}

func (c sqlClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}

	return nil
}
