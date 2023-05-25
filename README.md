# Conduit Connector for Databricks
test change
A [Conduit](https://conduit.io) destination connector for [Databricks](https://www.databricks.com/).

## How to build?
Run `make build` to build the connector.

## Testing
The repository contains unit and integrations tests for the connector. The integration tests need the following 
environment variables:
* `DATABRICKS_API_TOKEN`
* `DATABRICKS_HOST`
* `DATABRICKS_PORT`
* `DATABRICKS_HTTP_PATH`

If those are not set, the integration tests will be skipped. The following commands can be used to run all the tests:
```shell
export DATABRICKS_API_TOKEN="dapi123abc"
export DATABRICKS_HOST="dbc-123-abc.cloud.databricks.com"
export DATABRICKS_PORT="443"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/123abc"
make test
```

## Destination

### Configuration

| name  | description                                                                                                            | required | default value |
|-------|------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `dsn` | [DSN connection string](https://docs.databricks.com/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string) | true     | ""            |
