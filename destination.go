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
//go:generate mockgen -destination=mock/client.go -package=mock -mock_names=Client=Client . Client

import (
	"context"
	"fmt"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
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
	// Default table to which records will be written
	TableName string `json:"tableName" validate:"required"`
}

type Client interface {
	Open(context.Context, Config) error
	Close() error

	Insert(ctx context.Context, record opencdc.Record) error
	Update(ctx context.Context, record opencdc.Record) error
	Delete(ctx context.Context, record opencdc.Record) error
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

func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		sdk.Logger(ctx).Error().Msgf("Invalid config: %v", err)
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("opening the connector")

	if err := d.client.Open(ctx, d.config); err != nil {
		return fmt.Errorf("failed opening client: %w", err)
	}

	sdk.Logger(ctx).Info().Msgf("Connected to Databricks at %s:%d", d.config.Host, d.config.Port)

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	sdk.Logger(ctx).Trace().Msgf("writing %v records", len(records))
	sdk.Logger(ctx).Info().Msgf("Writing records to Databricks at %s:%d", d.config.Host, d.config.Port)

	for i, record := range records {
		sdk.Logger(ctx).Info().Msgf("Writing record %d to Databricks at %s:%d", i, d.config.Host, d.config.Port)
		sdk.Logger(ctx).Info().Msgf("Record: %v", record)
		sdk.Logger(ctx).Info().Msgf("Record operation: %v", record.Operation)
		sdk.Logger(ctx).Info().Msgf("Record key: %v", record.Key)
		sdk.Logger(ctx).Info().Msgf("Record payload: %v", record.Payload)
		sdk.Logger(ctx).Info().Msgf("Record payload before: %v", record.Payload.Before)
		sdk.Logger(ctx).Info().Msgf("Record payload after: %v", record.Payload.After)
		// sdk.Logger(ctx).Info().Msgf("Record payload before bytes: %v", record.Payload.Before.Bytes())
		// sdk.Logger(ctx).Info().Msgf("Record payload after bytes: %v", record.Payload.After.Bytes())
		// sdk.Logger(ctx).Info().Msgf("Record payload before string: %v", string(record.Payload.Before.Bytes()))
		// sdk.Logger(ctx).Info().Msgf("Record payload after string: %v", string(record.Payload.After.Bytes()))
		// sdk.Logger(ctx).Info().Msgf("Record metadata: %v", record.Metadata)

		// // Add base64 decoding for payload
		// if record.Payload.After != nil && len(record.Payload.After.Bytes()) > 0 {
		// 	decoded, err := base64.StdEncoding.DecodeString(string(record.Payload.After.Bytes()))
		// 	if err != nil {
		// 		sdk.Logger(ctx).Info().Msgf("Failed to decode payload after as base64: %v", err)
		// 	} else {
		// 		sdk.Logger(ctx).Info().Msgf("Record payload after (base64 decoded): %v", string(decoded))
		// 	}
		// }

		// if record.Payload.Before != nil && len(record.Payload.Before.Bytes()) > 0 {
		// 	decoded, err := base64.StdEncoding.DecodeString(string(record.Payload.Before.Bytes()))
		// 	if err != nil {
		// 		sdk.Logger(ctx).Info().Msgf("Failed to decode payload before as base64: %v", err)
		// 	} else {
		// 		sdk.Logger(ctx).Info().Msgf("Record payload before (base64 decoded): %v", string(decoded))
		// 	}
		// }

		err := sdk.Util.Destination.Route(
			ctx,
			record,
			d.client.Insert,
			d.client.Update,
			d.client.Delete,
			d.client.Insert,
		)
		if err != nil {
			sdk.Logger(ctx).Error().Msgf("Unable to handle record: %v", err)
			return i, fmt.Errorf("unable to handle record: %w", err)
		}
	}
	sdk.Logger(ctx).Info().Msgf("Wrote %d records", len(records))
	return len(records), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("tearing down the connector")
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
