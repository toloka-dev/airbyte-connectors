# Toloka connector image overlays

Standalone image overlays for the pinned vendor connector images. These are built
from each subdirectory's Dockerfile, independently of this fork's older Gradle/CDK
source tree. No dependencies are installed or upgraded. The stock connector
definitions and their metadata are unchanged. The upstream connector CI/publish
pipeline does not build or publish these overlays; run the documented offline
probes and publish through the reviewed operator procedure.

- `source-bigquery-wif`: BigQuery 0.4.4 using Google ADC and a projected Kubernetes token.
- `source-s3-cur`: S3 4.14.1 using AWS web-identity credentials for the CUR prefix.

Test target: https://airbyte-main.tlk-dp-prod.toloka.network/ on
`prod-tlk-aks-cluster`, namespace `airbyte-main`, ServiceAccount `airbyte-admin`.
On 2026-11-18 existing source/read and destination/write pods had the workload
identity label, Azure token environment and a projected token volume. Metadata
inspection does not prove the token can be read by these images or that AWS/GCP
accept it. Inspect each new check/discover/read pod and perform live checks.

The new Airbyte in namespace `airbyte` still lacked token injection on that date.
Do not switch the test connection or widen trust to that namespace without a new
review and pod verification. Neither overlay fixes the shared Airbyte launcher.

Infrastructure is managed separately in `toloka-dev/toloka-infrastructure`:
- GCP: `gcp/projects/gemini-api-finops-1052/airbyte-billing/` (PR #3836).
- AWS: `aws/toloka-doo/cur-export-reader/` (PR #3835).

Required network changes are in `toloka-dev/gitops`, production Cilium policy for
namespace `airbyte-main`. Do not start a sync or change existing connections as
part of a source-check test. This branch publishes no image automatically.
