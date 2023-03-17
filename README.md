# Conduit Connector for Databricks
A [Conduit](https://conduit.io) destination connector for [Databricks](https://www.databricks.com/).

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test` to run all the unit tests.

## Destination

### Configuration

| name  | description                                                                                                            | required | default value |
|-------|------------------------------------------------------------------------------------------------------------------------|----------|---------------|
| `dsn` | [DSN connection string](https://docs.databricks.com/dev-tools/go-sql-driver.html#connect-with-a-dsn-connection-string) | true     | ""            |
