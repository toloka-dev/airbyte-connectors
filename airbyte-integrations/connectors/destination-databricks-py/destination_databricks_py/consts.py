import dbxio

FIELD_AB_ID = "_airbyte_ab_id"
FIELD_DATA = "_airbyte_data"
FIELD_EMITTED_AT = "_airbyte_emitted_at"

DEST_SCHEMA = dbxio.TableSchema(
    [
        {"name": FIELD_AB_ID, "type": dbxio.types.StringType()},
        {"name": FIELD_DATA, "type": dbxio.types.StringType()},
        {"name": FIELD_EMITTED_AT, "type": dbxio.types.TimestampType()},
    ]
)
