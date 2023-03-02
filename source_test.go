package databricks_test

import (
	"context"
	"testing"

	databricks "github.com/conduitio-labs/conduit-connector-databricks"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	con := databricks.NewSource()
	err := con.Teardown(context.Background())
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
