# AWS CUR S3 connector image

Separate test source on https://airbyte-main.tlk-dp-prod.toloka.network/,
namespace `airbyte-main`, ServiceAccount `airbyte-admin` in `prod-tlk-aks-cluster`.
Requires the matching subject trust from infrastructure PR #3835 and reviewed
`airbyte-main` Cilium egress. Existing old-Airbyte job pods have projected-token
volumes; readable token and protocol compatibility with this image still need live
check/discover/read verification. No production sync is enabled by this change.


Uses the `airbyte-main` namespace's projected Kubernetes token to assume
`arn:aws:iam::863619919619:role/tdp-cur-export-reader` directly with AWS STS.
The matching AWS trust/policy and full acceptance checklist are in
`toloka-dev/toloka-infrastructure`, `aws/toloka-doo/cur-export-reader/README.md`.

The upstream image is pinned to S3 4.14.1 and its published digest. Its real
`SourceS3.create().check` and boto3 code use the default AWS credential chain when
the three credential form fields are omitted. The image sets `AWS_ROLE_ARN` and
`AWS_WEB_IDENTITY_TOKEN_FILE`; it does not call `sts:AssumeRole` or need a base key.
Metadata fallback is disabled. The inherited entrypoint is unchanged.

Disables unstructured telemetry and avoids NLTK
downloads during import. CUR is parsed with PyArrow, not unstructured/NLTK. No new
packages are installed. Keep this a separate custom source, not a replacement for
the stock S3 connector. Nothing in this directory is automatically built/deployed.

After infrastructure/network review and apply, build/publish from this directory:

```sh
az acr build --registry tlkprodregistry \
  --image tlk-airbyte/source-s3-cur:4.14.1-wi --platform linux/amd64 .
```

Register a custom source definition pointing at
`tlkprodregistry.azurecr.io/tlk-airbyte/source-s3-cur:4.14.1-wi`.
Use the published digest when deploying if supported by the definition.
For configuration, deployment prerequisites and live acceptance checks, see the
AWS root README. A missing projected token or an STS refusal must fail the sync;
do not work around it by adding a permanent key.

## Offline verification

Run from this directory. The synthetic token is not a credential; there is no
network, no real AWS credentials, and no production data in this probe.

```sh
docker build --platform linux/amd64 --network none --pull=false \
  -t local/airbyte-source-s3-cur:4.14.1 .
probe_dir=$(mktemp -d)
printf '%s' offline-test-token > "$probe_dir/fake.jwt"
docker run --rm --network none --platform linux/amd64 \
  --entrypoint python -e PYTHONPATH=/airbyte/integration_code \
  -v "$probe_dir/fake.jwt:/var/run/secrets/azure/tokens/azure-identity-token:ro" \
  -v "$PWD/test_auth.py:/tmp/test_auth.py:ro" \
  local/airbyte-source-s3-cur:4.14.1 /tmp/test_auth.py
rm -r "$probe_dir"
```

The probe enters the source's public factory/check, intercepts only SDK HTTP
transport, verifies `AssumeRoleWithWebIdentity` reads the projected token, verifies
signed prefix-scoped S3 requests, and parses synthetic Snappy Parquet. It cannot
prove live pod mutation, AWS OIDC trust, network reachability or ClickHouse sync.
