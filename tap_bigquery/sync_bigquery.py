import copy, datetime, json, time
import dateutil.parser
from decimal import Decimal

import simplejson
from os import environ
import singer
import singer.metrics as metrics

from google.cloud import bigquery

from . import utils
import getschema



LOGGER = utils.get_logger(__name__)

# StitchData compatible timestamp meta data
#  https://www.stitchdata.com/docs/data-structure/system-tables-and-columns
# The timestamp of the record extracted from the source
EXTRACT_TIMESTAMP = "_sdc_extracted_at"
# The timestamp of the record submit to the destination
# (kept null at extraction)
BATCH_TIMESTAMP = "_sdc_batched_at"
# Legacy timestamp field
LEGACY_TIMESTAMP = "_etl_tstamp"

BOOKMARK_KEY_NAME = "last_update"

SERVICE_ACCOUNT_INFO_ENV_VAR = "GOOGLE_APPLICATION_CREDENTIALS_STRING"

def _get_bigquery_client() -> bigquery.Client:
    """Initialize a bigquery client from credentials file JSON,
    if in environment, else credentials file.

    Returns:
        Initialized BigQuery client.
    """
    credentials_json = environ.get(SERVICE_ACCOUNT_INFO_ENV_VAR)
    if credentials_json:
        return bigquery.Client.from_service_account_info(json.loads(credentials_json))
    return bigquery.Client()

def _build_query(keys, filters=[], inclusive_start=True, limit=None, datetime_format="date-time", include_null_timestamps=False):
    keys = copy.deepcopy(keys)
    columns = ",".join(keys["columns"])
    datetime_key = keys.get("datetime_key")
    if "*" not in columns and datetime_key is not None and datetime_key not in columns:
        columns = columns + "," + datetime_key
    keys["columns"] = columns

    query = "SELECT {columns} FROM {table} WHERE 1=1".format(**keys)

    def _parse_integer_timestamp_as_datetime(field: str) -> str:
        return f"TIMESTAMP_SECONDS(COALESCE(SAFE_CAST(SUBSTR(CAST({field} AS STRING), 1, 10) AS INT64), 0))"

    start_datetime = keys.get("start_datetime")
    end_datetime = keys.get("end_datetime")
    if datetime_format == "integer":
        # Format integer column into trimmed timestamp.
        if keys.get("datetime_key"):
            keys["datetime_key"] = _parse_integer_timestamp_as_datetime(datetime_key)

    if start_datetime:
        if isinstance(start_datetime, int):
            keys["start_datetime"] = _parse_integer_timestamp_as_datetime(start_datetime)
        else:
            keys["start_datetime"] = f"TIMESTAMP '{keys['start_datetime']}'"

    if end_datetime:
        if isinstance(end_datetime, int):
            keys["end_datetime"] = _parse_integer_timestamp_as_datetime(end_datetime)
        else:
            keys["end_datetime"] = f"TIMESTAMP '{keys['end_datetime']}'"

    if filters:
        for f in filters:
            query = query + " AND " + f

    if keys.get("datetime_key"):
        datetime_conditions = []
        if keys.get("start_datetime") and keys.get("end_datetime"):
            # Both start and end datetime conditions must be met
            datetime_conditions.append(f"CAST({keys['datetime_key']} as datetime) >= CAST({keys['start_datetime']} as datetime)")
            datetime_conditions.append(f"CAST({keys['datetime_key']} as datetime) <= CAST({keys['end_datetime']} as datetime)")

            # Add OR condition to include rows where datetime_key is null
            if include_null_timestamps:
                datetime_conditions = [f"({' AND '.join(datetime_conditions)}) OR {datetime_key} IS NULL"]

            query += f" AND {' AND '.join(datetime_conditions)}"

    if keys.get("datetime_key"):
        query = (query + " ORDER BY {datetime_key} NULLS FIRST".format(**keys))

    if limit is not None:
        query = query + f" LIMIT {limit}"

    return query

def _build_field_schema(field):
    """
    Builds the schema for an individual BigQuery field.

    Args:
        field (bigquery.SchemaField): The BigQuery field.

    Returns:
        dict: JSONSchema-compliant field schema.
    """
    # Determine the base type
    base_type = _bigquery_field_to_json_type(field.field_type)

    # Build the field schema, making all fields nullable.
    field_schema = {
        "type": ["null", base_type],
        "description": field.description or ""
    }

    # Add format for specific field types
    if field.field_type.upper() in {"DATETIME", "DATE", "TIME"}:
        field_schema["format"] = _bigquery_field_to_format(field.field_type)

    # Add support for arrays if field.mode == "REPEATED"
    if field.mode.upper() == "REPEATED":
        field_schema = {
            "type": "array",
            "items": field_schema
        }

    return field_schema

def _bigquery_field_to_json_type(bq_type):
    """
    Converts a BigQuery field type to a JSONSchema type.

    Args:
        bq_type (str): The BigQuery field type (e.g., STRING, INTEGER).

    Returns:
        str: Equivalent JSONSchema type.
    """
    type_mapping = {
        "STRING": "string",
        "BYTES": "string",
        "INTEGER": "integer",
        "INT64": "integer",
        "FLOAT": "number",
        "FLOAT64": "number",
        "BOOLEAN": "boolean",
        "BOOL": "boolean",
        "TIMESTAMP": "string",  # Format can be refined further to include date-time
        "DATE": "string",
        "TIME": "string",
        "DATETIME": "string",
        "GEOGRAPHY": "string",
        "RECORD": "object",  # Nested fields
    }
    return type_mapping.get(bq_type.upper(), "string")  # Default to string

def _bigquery_field_to_format(bq_type):
    """
    Maps BigQuery field types to JSONSchema formats.

    Args:
        bq_type (str): The BigQuery field type.

    Returns:
        str: JSONSchema format for the given field type.
    """
    format_mapping = {
        "DATETIME": "date-time",
        "DATE": "date",
        "TIME": "time"
    }
    return format_mapping.get(bq_type.upper())

def do_discover(config, stream, output_schema_file=None,
                add_timestamp=True):
    client = _get_bigquery_client()

    table_name = stream["table"]
    column_names = stream["columns"]

    # Filter grave characters from table name and query for schema.
    table = client.get_table(table_name.replace("`", ""))

    schema = {
        "type": "object",
        "properties": {
            field.name.lower(): _build_field_schema(field)
            for field in table.schema
        }
    }

    if column_names:
        schema["properties"] = {
            k: v
            for k, v
            in schema["properties"].items()
            if k in map(lambda x: x.lower(), column_names)
        }

    if add_timestamp:
        timestamp_format = {"type": ["null", "string"],
                            "format": "date-time"}
        schema["properties"][EXTRACT_TIMESTAMP] = timestamp_format
        schema["properties"][BATCH_TIMESTAMP] = timestamp_format
        # Support the legacy field
        schema["properties"][LEGACY_TIMESTAMP] = {"type": ["null", "number"],
                                                  "inclusion": "automatic"}

    if output_schema_file:
        with open(output_schema_file, "w") as f:
            json.dump(schema, f, indent=2)

    stream_metadata = [{
        "metadata": {
            "selected": True,
            "table": stream["table"],
            "columns": stream["columns"],
            "filters": stream.get("filters", []),
            "datetime_key": stream["datetime_key"],
        },
        "breadcrumb": []
    }]

    key_properties = []
    catalog = {"selected": True,
               "type": "object",
               "stream": stream["name"],
               "key_properties": key_properties,
               "properties": schema["properties"]
               }

    return stream_metadata, key_properties, catalog


def _convert_value(value, prop):
    """Helper function to convert a single value according to schema property."""
    if value is None:
        if prop.type[0] != "null":
            raise ValueError("NULL value not allowed by the schema")
        return None
        
    if prop.format == "date-time":
        if type(value) == str:
            r = dateutil.parser.parse(value)
        elif type(value) == datetime.date:
            r = datetime.datetime(
                year=value.year,
                month=value.month,
                day=value.day)
        elif type(value) == datetime.datetime:
            r = value
        return r.isoformat()
    elif prop.type[1] == "string":
        return str(value)
    elif prop.type[1] == "number":
        return Decimal(value)
    elif prop.type[1] == "integer":
        return int(value)
    return value


def _replace_decimal_zero_with_none(obj):
    if isinstance(obj, dict):
        return {k: _replace_decimal_zero_with_none(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_replace_decimal_zero_with_none(elem) for elem in obj]
    elif isinstance(obj, Decimal) and obj == Decimal('0.0'):
        return None
    else:
        return obj


def do_sync(config, state, stream):
    singer.set_currently_syncing(state, stream.tap_stream_id)
    singer.write_state(state)

    client = _get_bigquery_client()
    metadata = stream.metadata[0]["metadata"]
    tap_stream_id = stream.tap_stream_id

    # Get datetime key type from stream schema to determine query format.
    datetime_key_schema = stream.schema.properties[metadata["datetime_key"]]
    datetime_key_types = datetime_key_schema.type

    if "integer" in datetime_key_types:
        datetime_key_format = "integer"
    else:
        datetime_key_format = "date-time"

    inclusive_start = True
    start_datetime = singer.get_bookmark(state, tap_stream_id,
                                         BOOKMARK_KEY_NAME)
    if start_datetime:
        # Do not include NULLs on subsequent runs.
        include_null_timestamps = False
        if not config.get("start_always_inclusive"):
            inclusive_start = False
    else:
        # Include NULLs on first run.
        include_null_timestamps = True
        start_datetime = config.get("start_datetime")
        if datetime_key_format == "date-time":
            start_datetime = dateutil.parser.parse(start_datetime).strftime(
                    "%Y-%m-%d %H:%M:%S.%f")

    end_datetime = None
    if config.get("end_datetime"):
        end_datetime = config.get("end_datetime")
        if datetime_key_format == "date-time":
            end_datetime = dateutil.parser.parse(
                config.get("end_datetime")).strftime("%Y-%m-%d %H:%M:%S.%f")

    singer.write_schema(tap_stream_id, stream.schema.to_dict(),
                        stream.key_properties)

    keys = {"table": metadata["table"],
            "columns": metadata["columns"],
            "datetime_key": metadata.get("datetime_key"),
            "start_datetime": start_datetime,
            "end_datetime": end_datetime
            }

    limit = config.get("limit", None)

    query = _build_query(keys, metadata.get("filters", []), inclusive_start,
                         limit=limit, datetime_format=datetime_key_format, 
                         include_null_timestamps=include_null_timestamps)
    query_job = client.query(query)

    properties = stream.schema.properties
    last_update = start_datetime

    LOGGER.info("Running query:\n    %s" % query)

    extract_tstamp = datetime.datetime.utcnow()
    extract_tstamp = extract_tstamp.replace(tzinfo=datetime.timezone.utc)

    with metrics.record_counter(tap_stream_id) as counter:
        for row in query_job:
            record = {}
            for key in properties.keys():
                prop = properties[key]

                if key in [LEGACY_TIMESTAMP,
                           EXTRACT_TIMESTAMP,
                           BATCH_TIMESTAMP]:
                    continue

                value = row[key]
                
                # Handle array types.
                if isinstance(value, list):
                    if 'array' in prop.type:
                        items_prop = prop.items
                        record[key] = [_convert_value(item, items_prop) for item in value]
                    else:
                        raise ValueError(f"Got list value for non-array field {key}")
                else:
                    record[key] = _convert_value(value, prop)

            # Replace all Decimal(0.0) values with None (including in lists).
            record = _replace_decimal_zero_with_none(record)

            if LEGACY_TIMESTAMP in properties.keys():
                record[LEGACY_TIMESTAMP] = int(round(time.time() * 1000))
            if EXTRACT_TIMESTAMP in properties.keys():
                record[EXTRACT_TIMESTAMP] = extract_tstamp.isoformat()

            singer.write_record(stream.stream, record)

            last_update = record[keys["datetime_key"]]
            counter.increment()

    state = singer.write_bookmark(state, tap_stream_id, BOOKMARK_KEY_NAME,
                                  last_update)

    singer.write_state(state)
