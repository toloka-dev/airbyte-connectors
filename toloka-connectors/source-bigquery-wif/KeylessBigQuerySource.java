package io.airbyte.integrations.source.bigquery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.db.bigquery.BigQueryDatabase;
import io.airbyte.cdk.integrations.base.IntegrationRunner;
import io.airbyte.protocol.models.v0.ConnectorSpecification;

public final class KeylessBigQuerySource extends BigQuerySource {
  @Override
  public JsonNode toDatabaseConfig(JsonNode config) {
    return super.toDatabaseConfig(withApplicationDefaultCredentials(config));
  }

  @Override
  protected BigQueryDatabase createDatabase(JsonNode config) {
    return super.createDatabase(withApplicationDefaultCredentials(config));
  }

  private static JsonNode withApplicationDefaultCredentials(JsonNode config) {
    ObjectNode adcConfig = config.deepCopy();
    adcConfig.put(CONFIG_CREDS, "");
    return adcConfig;
  }

  @Override
  public ConnectorSpecification spec() throws Exception {
    ConnectorSpecification spec = super.spec();
    ObjectNode schema = (ObjectNode) spec.getConnectionSpecification();
    ((ObjectNode) schema.get("properties")).remove(CONFIG_CREDS);
    ArrayNode required = (ArrayNode) schema.get("required");
    if (required != null) {
      for (int i = required.size() - 1; i >= 0; i--) {
        if (CONFIG_CREDS.equals(required.get(i).asText())) {
          required.remove(i);
        }
      }
    }
    return spec;
  }

  public static void main(String[] args) throws Exception {
    new IntegrationRunner(new KeylessBigQuerySource()).run(args);
  }
}
