# pynomina
Simple analysis of the Paraguayan official officers, data source: https://datos.hacienda.gov.py/data/nomina/descargas

## Config File

Add a config.toml using the following content:

```toml
[postgres]
HOST = "localhost"
PORT = "5432"
DB = "your_postgres_db_name"
SCHEMA = "your_postgres_schema_name"
USER = "your_postgres_user"
PASS = "your_postgres_pass"

[nominas]
RESOURCE = "https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos"
ST_MONTH_YEAR = "2013-01"
FORCE_DOWNLOAD = false
```
