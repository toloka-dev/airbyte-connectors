FROM airbyte/source-bigquery:0.4.4@sha256:f3bcf0604f9cc5bb8ddfa2914816afcd2411f0078de798bad72619e87b5b6849

COPY KeylessBigQuerySource.java /tmp/wif-src/KeylessBigQuerySource.java
RUN mkdir -p /tmp/wif-classes \
    && javac -proc:none -cp '/airbyte/lib/*' -d /tmp/wif-classes /tmp/wif-src/KeylessBigQuerySource.java \
    && jar uf /airbyte/lib/io.airbyte.airbyte-integrations.connectors-source-bigquery.jar -C /tmp/wif-classes . \
    && cp /airbyte/bin/source-bigquery /airbyte/bin/source-bigquery-wif \
    && sed -i 's/io.airbyte.integrations.source.bigquery.BigQuerySource/io.airbyte.integrations.source.bigquery.KeylessBigQuerySource/g' /airbyte/bin/source-bigquery-wif \
    && rm -rf /tmp/wif-src /tmp/wif-classes

COPY gcp-wif.json /airbyte/gcp-wif.json
ENV APPLICATION=source-bigquery-wif \
    GOOGLE_APPLICATION_CREDENTIALS=/airbyte/gcp-wif.json
