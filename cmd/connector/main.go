package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	databricks "github.com/conduitio-labs/conduit-connector-databricks"
)

func main() {
	sdk.Serve(databricks.Connector)
}
