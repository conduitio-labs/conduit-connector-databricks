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

//go:generate paramgen -output=paramgen_config.go Config
//go:generate mockgen -destination=mock/client.go -package=mock -mock_names=Client=Client . Client

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Config struct {
	// Personal access token.
	Token string `json:"token" validate:"required"`
	// Databricks server hostname
	Host string `json:"host" validate:"required"`
	// Databricks port
	Port int `json:"port" default:"443"`
	// Databricks compute resources URL
	HTTPath string `json:"httpPath" validate:"required"`
	//Table name
	TableName string `json:"tableName" validate:"required"`
}

type Client interface {
	Open(context.Context, Config) error
	Close() error
	HandleRecord(ctx context.Context, record sdk.Record) error
}

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client Client
}

func NewDestination() sdk.Destination {
	return NewDestinationWithClient(newClient())
}

func NewDestinationWithClient(c Client) sdk.Destination {
	return sdk.DestinationWithMiddleware(
		&Destination{client: c},
	)
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

	d.client = newClient()

	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("opening the connector")

	if err := d.client.Open(ctx, d.config); err != nil {
		return fmt.Errorf("failed opening client: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	sdk.Logger(ctx).Trace().Msgf("writing %v records", len(records))

	for i, record := range records {

		err := d.client.HandleRecord(ctx, record)
		if err != nil {
			return i, fmt.Errorf("unable to handle record: %w", err)
		}
		//write the data to databricks
		/*payload, ok := record.Payload.After.Bytes().(map[string]interface{})
		if !ok {
			return payload, nil
		}
		*/
	}

	return 0, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("tearing down the connector")
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
