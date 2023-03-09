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

//go:generate paramgen -output=paramgen_dest.go Config

import (
	"context"
	"database/sql"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	_ "github.com/databricks/databricks-sql-go"
)

type Config struct {
	// DSN is a data source name connection string
	// https://docs.databricks.com/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string
	DSN string `json:"dsn" validate:"required"`
}

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client sqlClient
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config
}

func NewDestination() sdk.Destination {
	// Create Destination and wrap it in the default middleware.
	return sdk.DestinationWithMiddleware(&Destination{})
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	db, err := sql.Open("databricks", d.config.DSN)
	if err != nil {
		return fmt.Errorf("failed opening database: %w", err)
	}
	if err = db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed pinging database: %w", err)
	}
	d.client = newClient(db)
	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	sdk.Logger(ctx).Info().Msgf("writing %v records", len(records))
	return 0, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	return d.client.Close()
}
