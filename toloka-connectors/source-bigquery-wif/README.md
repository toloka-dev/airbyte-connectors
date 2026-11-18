# BigQuery 0.4.4 with Application Default Credentials

For a separate test source on https://airbyte-main.tlk-dp-prod.toloka.network/
in namespace `airbyte-main` on `prod-tlk-aks-cluster`.
This is a separate custom source; the stock BigQuery definition is unchanged.
The upstream image is pinned to the version observed in that Airbyte and its digest.
No libraries are installed or upgraded.

`KeylessBigQuerySource` inherits the original queries, discovery and schema mapping.
It removes the service-account key field from the form, supplies an empty internal
key value in both raw and normalized database-config paths, and uses the stock
`BigQueryDatabase` ADC branch. The SDK reads the non-secret `gcp-wif.json` via
`GOOGLE_APPLICATION_CREDENTIALS`, exchanges the projected Kubernetes JWT at Google
STS, then obtains an OAuth access token for the existing billing service account.

The subclass is compiled with the image's own JDK and libraries, added to the
connector jar, and invoked through a copy of the original Gradle launcher. The
inherited `javabase.sh` and its JVM options are retained. The original application
and every unrelated source are unaffected. Build context must be this directory.

## Before production use

The old Airbyte has projected-token volumes in existing job pods. After registering
this separate source, run check/discovery and verify each new job pod uses `airbyte-main/airbyte-admin` and has the readable file
`/var/run/secrets/azure/tokens/azure-identity-token`; never print the token. The newer
Airbyte in namespace `airbyte` still lacks this injection. Old platform compatibility
with this separately registered 0.4.4 image remains a live acceptance check.

Apply the reviewed GCP federation and `airbyte-main` STS/IAM Credentials egress first.
The full acceptance/rollback checklist is in `toloka-dev/toloka-infrastructure`,
`gcp/projects/gemini-api-finops-1052/airbyte-billing/README.md` (PR #3836).

After infrastructure/network apply and code review, publish from this directory:

```sh
az acr build --registry tlkprodregistry \
  --image tlk-airbyte/source-bigquery-wif:0.4.4 --platform linux/amd64 .
```

Register the custom source with image
`tlkprodregistry.azurecr.io/tlk-airbyte/source-bigquery-wif:0.4.4`, pinned to the
published digest when supported. Set project ID `gemini-api-finops-1052` and
dataset ID `gemini_billing_data`. No key is required. Verify the actual destination
namespace and selected billing stream before syncing. Rebuild after any WIF
provider/service-account routing change. Before publishing, compare this directory's
`gcp-wif.json` byte-for-byte with the Terraform root's `gcp-wif.json` in PR #3836,
and compare its fields to Terraform's `airbyte_external_account_config` output after
apply. The infrastructure root cannot inspect this separate repository or a
published image. Deploy the verified image digest; a mutable tag is not that check.

## Offline compatibility probe

Run from this directory with the pinned upstream image already cached:

```sh
docker build --platform linux/amd64 --network none --pull=false \
  -t local/airbyte-source-bigquery-wif:0.4.4 .
probe_dir=$(mktemp -d)
printf '%s' offline-projected-token > "$probe_dir/fake.jwt"
docker run --rm --hostname localhost --network none --platform linux/amd64 \
  --entrypoint sh \
  -v "$probe_dir/fake.jwt:/var/run/secrets/azure/tokens/azure-identity-token:ro" \
  -v "$PWD/tests:/tests:ro" \
  local/airbyte-source-bigquery-wif:0.4.4 /tests/run-offline.sh
rm -r "$probe_dir"
docker run --rm --network none --platform linux/amd64 \
  local/airbyte-source-bigquery-wif:0.4.4 spec
```

The test runs only in a disposable container. It adds its harness to that
container's jar and uses the exact ordered classpath of the production launcher;
the upstream image has conflicting library versions under a naive wildcard
runtime classpath. No test classes are shipped in the production image.

The probe proves that stock key parsing rejects WIF input, then enters the real
custom source's public `check` with no key in its configuration. It intercepts
only HTTPS transport: the actual Google Auth 1.16.0 library reads the synthetic
projected token, performs STS and service-account impersonation, and the actual
BigQuery source runs both SELECT check operations. Requests must target the pinned
provider, account and project and use the expected synthetic bearer tokens.
The container has no network or real credentials. A separate real Docker `spec`
invocation verifies startup dispatch and the key-free form.

This does not prove live GCP IAM/organization-policy acceptance, pod mutation,
network reachability, token rotation, full data read/discovery or ClickHouse sync.
Those remain explicit operator acceptance checks before enablement.
The pinned Google Auth 1.16.0 parser rejected fractional-second `expireTime` in a
synthetic IAM response; the passing fixture uses whole-second expiry. Check the
real IAM response's compatibility before enabling this legacy SDK, rather than
treating the synthetic response as proof of production compatibility.
