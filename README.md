# Conduit Connector for Databricks
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
| `token` | Personal access token | true     |             |
| `host` | Databricks server hostname | true     |             |
| `port` | Databricks port | false     | `443`            |
| `httpPath` | Databricks compute resources URL | true     |             |
| `tableName` | Default table to which records will be written | true     |             |

![scarf pixel](https://static.scarf.sh/a.png?x-pxid=ebac069a-c2dc-45b5-aaf1-785790bd20c5)