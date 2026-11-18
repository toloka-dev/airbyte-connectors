#!/bin/sh
set -eu

mkdir -p /tmp/probe-classes
javac -proc:none -cp '/airbyte/lib/*' -d /tmp/probe-classes /tests/BigQueryWifProbe.java
jar uf /airbyte/lib/io.airbyte.airbyte-integrations.connectors-source-bigquery.jar -C /tmp/probe-classes .
sed 's/io.airbyte.integrations.source.bigquery.KeylessBigQuerySource/BigQueryWifProbe/g' /airbyte/bin/source-bigquery-wif > /airbyte/bin/wif-probe
chmod +x /airbyte/bin/wif-probe
exec /airbyte/bin/wif-probe
