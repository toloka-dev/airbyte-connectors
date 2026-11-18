import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airbyte.cdk.db.bigquery.BigQueryDatabase;
import io.airbyte.integrations.source.bigquery.KeylessBigQuerySource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.HttpsURLConnection;

public final class BigQueryWifProbe {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String PROJECT = "gemini-api-finops-1052";
  private static final List<String> CALLS = new ArrayList<>();
  private static final Map<String, JsonNode> JOBS = new HashMap<>();

  public static void main(String[] args) throws Exception {
    URL.setURLStreamHandlerFactory(protocol -> "https".equals(protocol) ? new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(URL url) {
        return new OfflineConnection(url);
      }
    } : null);

    String wif = Files.readString(Path.of(System.getenv("GOOGLE_APPLICATION_CREDENTIALS")));
    try {
      new BigQueryDatabase(PROJECT, wif);
      throw new AssertionError("Stock 0.4.4 unexpectedly accepted external_account in the key field");
    } catch (RuntimeException expected) {
      if (!(expected.getCause() instanceof IOException)) {
        throw expected;
      }
    }

    KeylessBigQuerySource source = new KeylessBigQuerySource();
    JsonNode schema = source.spec().getConnectionSpecification();
    require(!schema.path("properties").has("credentials_json"), "Key field must not be exposed");
    for (JsonNode required : schema.path("required")) {
      require(!"credentials_json".equals(required.asText()), "Key field must not be required");
    }
    ObjectNode config = JSON.createObjectNode();
    config.put("project_id", PROJECT);
    config.put("dataset_id", "gemini_billing_data");
    var status = source.check(config);
    require("SUCCEEDED".equals(status.getStatus().toString()), "Source check failed: " + status);
    require(CALLS.contains("sts") && CALLS.contains("impersonate"), "Federation was not exercised");
    require(JOBS.size() >= 2, "Source did not run both actual BigQuery check queries");
    System.out.println("PASS: stock rejects WIF key input; custom Source.check -> ADC -> projected JWT -> STS -> impersonation -> BigQuery SELECT checks");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }

  private static final class OfflineConnection extends HttpsURLConnection {
    private final ByteArrayOutputStream request = new ByteArrayOutputStream();
    private byte[] response;

    OfflineConnection(URL url) {
      super(url);
    }

    private byte[] response() throws IOException {
      if (response != null) {
        return response;
      }
      String host = url.getHost();
      String path = url.getPath();
      byte[] requestBytes = request.toByteArray();
      if ("gzip".equalsIgnoreCase(getRequestProperty("Content-Encoding"))) {
        requestBytes = new GZIPInputStream(new ByteArrayInputStream(requestBytes)).readAllBytes();
      }
      String body = new String(requestBytes, StandardCharsets.UTF_8);
      if ("sts.googleapis.com".equals(host) && "/v1/token".equals(path)) {
        Map<String, String> fields = new HashMap<>();
        for (String pair : body.split("&")) {
          String[] parts = pair.split("=", 2);
          fields.put(URLDecoder.decode(parts[0], StandardCharsets.UTF_8), URLDecoder.decode(parts[1], StandardCharsets.UTF_8));
        }
        require("offline-projected-token".equals(fields.get("subject_token")), "SDK did not read projected JWT");
        require("urn:ietf:params:oauth:token-type:jwt".equals(fields.get("subject_token_type")), "Wrong subject token type");
        require("//iam.googleapis.com/projects/139456295637/locations/global/workloadIdentityPools/airbyte-it-billing/providers/prod-tlk-aks".equals(fields.get("audience")), "Wrong WIF provider");
        CALLS.add("sts");
        response = "{\"access_token\":\"offline-federated-token\",\"issued_token_type\":\"urn:ietf:params:oauth:token-type:access_token\",\"token_type\":\"Bearer\",\"expires_in\":3600}".getBytes(StandardCharsets.UTF_8);
      } else if ("iamcredentials.googleapis.com".equals(host)) {
        require(path.endsWith("/billing-export-pusher@gemini-api-finops-1052.iam.gserviceaccount.com:generateAccessToken"), "Wrong impersonated account");
        require("Bearer offline-federated-token".equals(getRequestProperty("Authorization")), "Wrong impersonation authorization");
        JsonNode scopes = JSON.readTree(body).path("scope");
        require("3600s".equals(JSON.readTree(body).path("lifetime").asText()), "Unexpected token lifetime");
        boolean hasQueryScope = false;
        for (JsonNode scope : scopes) {
          hasQueryScope |= "https://www.googleapis.com/auth/bigquery".equals(scope.asText())
              || "https://www.googleapis.com/auth/cloud-platform".equals(scope.asText());
        }
        require(hasQueryScope, "Impersonated token lacks a BigQuery scope");
        CALLS.add("impersonate");
        response = JSON.writeValueAsBytes(Map.of("accessToken", "offline-impersonated-token", "expireTime", java.time.Instant.now().plusSeconds(3600).truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString()));
      } else if ("www.googleapis.com".equals(host)) {
        require("Bearer offline-impersonated-token".equals(getRequestProperty("Authorization")), "BigQuery did not use impersonated credentials");
        require(path.startsWith("/bigquery/v2/projects/" + PROJECT + "/"), "Wrong BigQuery project");
        ObjectNode result = JSON.createObjectNode();
        if (path.endsWith("/jobs") && "POST".equals(getRequestMethod())) {
          JsonNode submitted = JSON.readTree(body);
          String query = submitted.path("configuration").path("query").path("query").asText();
          require(query.toLowerCase().startsWith("select 1"), "Unexpected data operation");
          String jobId = submitted.path("jobReference").path("jobId").asText();
          require(!jobId.isEmpty(), "Missing query job ID");
          result.set("jobReference", submitted.get("jobReference"));
          ObjectNode configuration = submitted.path("configuration").deepCopy();
          ((ObjectNode) configuration.get("query")).putObject("destinationTable")
              .put("projectId", PROJECT).put("datasetId", "_offline").put("tableId", jobId);
          result.set("configuration", configuration);
          result.putObject("status").put("state", "DONE");
          JOBS.put(jobId, result);
        } else if (path.contains("/queries/") || path.endsWith("/data")) {
          result.put("jobComplete", true);
          result.put("totalRows", "1");
          result.putObject("schema").putArray("fields").addObject().put("name", "f0_").put("type", "INTEGER");
          result.putArray("rows").addObject().putArray("f").addObject().put("v", "1");
        } else if (path.contains("/datasets/_offline/tables/")) {
          result.putObject("tableReference").put("projectId", PROJECT).put("datasetId", "_offline")
              .put("tableId", path.substring(path.lastIndexOf('/') + 1));
          result.putObject("schema").putArray("fields").addObject().put("name", "f0_").put("type", "INTEGER");
          result.put("type", "TABLE");
          result.put("numRows", "1");
        } else if (path.contains("/jobs/")) {
          JsonNode job = JOBS.get(path.substring(path.lastIndexOf('/') + 1));
          require(job != null, "Unknown job lookup");
          result.setAll((ObjectNode) job);
        } else {
          throw new AssertionError("Unexpected BigQuery endpoint: " + path);
        }
        CALLS.add("bigquery");
        response = JSON.writeValueAsBytes(result);
      } else {
        throw new AssertionError("Unexpected network endpoint: " + host + path);
      }
      return response;
    }

    @Override public void connect() {}
    @Override public void disconnect() {}
    @Override public boolean usingProxy() { return false; }
    @Override public String getCipherSuite() { return "offline"; }
    @Override public Certificate[] getLocalCertificates() { return new Certificate[0]; }
    @Override public Certificate[] getServerCertificates() { return new Certificate[0]; }
    @Override public OutputStream getOutputStream() { return request; }
    @Override public InputStream getInputStream() throws IOException { return new ByteArrayInputStream(response()); }
    @Override public int getResponseCode() throws IOException { response(); return 200; }
    @Override public String getResponseMessage() { return "OK"; }
    @Override public String getContentType() { return "application/json; charset=UTF-8"; }
    @Override public long getContentLengthLong() { try { return response().length; } catch (IOException e) { throw new RuntimeException(e); } }
    @Override public String getHeaderFieldKey(int n) { return n == 1 ? "Content-Type" : null; }
    @Override public String getHeaderField(int n) { return n == 0 ? "HTTP/1.1 200 OK" : n == 1 ? getContentType() : null; }
    @Override public String getHeaderField(String name) { return "Content-Type".equalsIgnoreCase(name) ? getContentType() : null; }
  }
}
