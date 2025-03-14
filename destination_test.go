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

package databricks_test

import (
	"context"
	"testing"

	databricks "github.com/conduitio-labs/conduit-connector-databricks"
	"github.com/conduitio-labs/conduit-connector-databricks/mock"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/mock/gomock"
)

func TestConfigure(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	client := mock.NewClient(gomock.NewController(t))
	cfgMap := map[string]string{"token": "test", "host": "test", "httpPath": "test", "tableName": "test"}
	var cfg databricks.Config
	err := sdk.Util.ParseConfig(ctx, cfgMap, &cfg, databricks.NewDestination().Parameters())
	is.NoErr(err)

	underTest := databricks.NewDestinationWithClient(client)
	err = underTest.Configure(ctx, cfgMap)
	is.NoErr(err)

	client.EXPECT().Open(gomock.Any(), cfg).Return(nil)
	err = underTest.Open(ctx)
	is.NoErr(err)
}

func TestTeardown_NoOpen(t *testing.T) {
	con := databricks.NewDestination()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
