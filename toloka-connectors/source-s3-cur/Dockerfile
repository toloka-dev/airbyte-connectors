FROM airbyte/source-s3:4.14.1@sha256:1adffaca8c598ca4e17d708292cacb28a10168d7796fe68809a73905b142d1f1

ENV DO_NOT_TRACK=true \
    AWS_ROLE_ARN=arn:aws:iam::863619919619:role/tdp-cur-export-reader \
    AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/secrets/azure/tokens/azure-identity-token \
    AWS_ROLE_SESSION_NAME=airbyte-aws-cur \
    AWS_DEFAULT_REGION=us-east-1 \
    AWS_STS_REGIONAL_ENDPOINTS=regional \
    AWS_EC2_METADATA_DISABLED=true

RUN python -c "import os, zipfile; d = '/airbyte/nltk_data/tokenizers'; os.makedirs(d, exist_ok=True); [zipfile.ZipFile(os.path.join(d, n + '.zip'), 'w').close() for n in ('punkt', 'punkt_tab', 'averaged_perceptron_tagger_eng')]"
