import datetime
import io
import logging
import os
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.awsrequest import AWSResponse
from botocore.response import StreamingBody

from source_s3.v4 import SourceS3


def main():
    assert (
        os.environ["AWS_ROLE_ARN"]
        == "arn:aws:iam::863619919619:role/tdp-cur-export-reader"
    )
    assert (
        os.environ["AWS_WEB_IDENTITY_TOKEN_FILE"]
        == "/var/run/secrets/azure/tokens/azure-identity-token"
    )
    assert os.environ["AWS_EC2_METADATA_DISABLED"] == "true"
    bucket = "toloka-cur-exports"
    key = "cur2-org/toloka-cur2-org/data/BILLING_PERIOD=2026-11/part.snappy.parquet"
    now = datetime.datetime.now(datetime.timezone.utc)
    buf = io.BytesIO()
    pq.write_table(
        pa.table({"line_item_unblended_cost": [1.25]}), buf, compression="snappy"
    )
    parquet = buf.getvalue()
    calls = []

    def get_response(endpoint, request, operation, context):
        calls.append(operation.name)
        if operation.name == "AssumeRoleWithWebIdentity":
            body = (
                request.body.decode()
                if isinstance(request.body, bytes)
                else request.body
            )
            fields = parse_qs(body)
            assert fields["RoleArn"] == [os.environ["AWS_ROLE_ARN"]]
            assert fields["WebIdentityToken"][0].strip() == "offline-test-token"
            response = {
                "Credentials": {
                    "AccessKeyId": "ASIAOFFLINETEST123456",
                    "SecretAccessKey": "x" * 40,
                    "SessionToken": "offline-session-token",
                    "Expiration": now + datetime.timedelta(hours=1),
                },
                "AssumedRoleUser": {
                    "AssumedRoleId": "offline:airbyte-aws-cur",
                    "Arn": "arn:aws:sts::863619919619:assumed-role/tdp-cur-export-reader/airbyte-aws-cur",
                },
            }
        else:
            assert endpoint.host == "https://s3.amazonaws.com"
            auth = request.headers.get("Authorization", b"")
            assert b"ASIAOFFLINETEST123456" in auth
            if operation.name == "ListObjectsV2":
                assert parse_qs(urlsplit(request.url).query)["prefix"][0].startswith(
                    "cur2-org/"
                )
                response = {
                    "IsTruncated": False,
                    "KeyCount": 1,
                    "Name": bucket,
                    "Contents": [
                        {
                            "Key": key,
                            "LastModified": now,
                            "Size": len(parquet),
                            "ETag": '"offline"',
                        }
                    ],
                }
            elif operation.name == "HeadObject":
                response = {"ContentLength": len(parquet), "LastModified": now}
            elif operation.name == "GetObject":
                payload = parquet
                byte_range = request.headers.get("Range")
                if byte_range:
                    if isinstance(byte_range, bytes):
                        byte_range = byte_range.decode()
                    start, end = byte_range.removeprefix("bytes=").split("-")
                    payload = parquet[int(start) : int(end) + 1 if end else None]
                response = {
                    "Body": StreamingBody(io.BytesIO(payload), len(payload)),
                    "ContentLength": len(payload),
                }
                if byte_range:
                    response["ContentRange"] = (
                        f"bytes {start}-{end or len(parquet) - 1}/{len(parquet)}"
                    )
            else:
                raise AssertionError(f"Unexpected AWS operation: {operation.name}")
        response["ResponseMetadata"] = {"HTTPStatusCode": 200, "RetryAttempts": 0}
        return (AWSResponse(request.url, 200, {}, None), response), None

    config = {
        "bucket": bucket,
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "aws_cur2",
                "globs": ["cur2-org/toloka-cur2-org/data/**/*.parquet"],
                "format": {"filetype": "parquet"},
                "validation_policy": "Emit Record",
            }
        ],
    }
    boto3.DEFAULT_SESSION = None
    with patch("botocore.endpoint.Endpoint._get_response", get_response):
        try:
            result = SourceS3.create().check(logging.getLogger("offline-probe"), config)
        except Exception as exc:
            raise AssertionError(getattr(exc, "message", str(exc))) from exc
    assert result.status.value == "SUCCEEDED", result
    assert "AssumeRoleWithWebIdentity" in calls, calls
    assert "GetObject" in calls, calls
    assert "AssumeRole" not in calls, calls
    print(
        "PASS: SourceS3.check -> web identity STS -> signed prefix-limited S3 -> Snappy Parquet"
    )


if __name__ == "__main__":
    main()
