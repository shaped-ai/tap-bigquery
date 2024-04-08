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

def get_bigquery_client():
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

def do_discover(config, stream, output_schema_file=None,
                add_timestamp=True):
    client = get_bigquery_client()

    keys = {"table": stream["table"],
            "columns": stream["columns"]
            }
    limit = config.get("limit", 25000)
    query = _build_query(keys, stream.get("filters"), limit=limit)

    LOGGER.info("Running query:\n    " + query)

    query_job = client.query(query)
    results = query_job.result()  # Waits for job to complete.

    data = []
    # Read everything upfront
    for row in results:
        record = {}
        for key in row.keys():
            record[key] = row[key]
        data.append(record)

    if not data:
        raise Exception("Cannot infer schema: No record returned.")

    schema = getschema.infer_schema(data)
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
            "datetime_key": stream["datetime_key"]
            # "inclusion": "available",
            # "table-key-properties": ["id"],
            # "valid-replication-keys": ["date_modified"],
            # "schema-name": "users"
            },
        "breadcrumb": []
        }]

    # TODO: Need to put something in here?
    key_properties = []

    catalog = {"selected": True,
               "type": "object",
               "stream": stream["name"],
               "key_properties": key_properties,
               "properties": schema["properties"]
               }

    return stream_metadata, key_properties, catalog


def do_sync(config, state, stream):
    singer.set_currently_syncing(state, stream.tap_stream_id)
    singer.write_state(state)

    client = get_bigquery_client()
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

                if row[key] is None:
                    if prop.type[0] != "null":
                        raise ValueError(
                            "NULL value not allowed by the schema"
                        )
                    else:
                        record[key] = None
                elif prop.format == "date-time":
                    if type(row[key]) == str:
                        r = dateutil.parser.parse(row[key])
                    elif type(row[key]) == datetime.date:
                        r = datetime.datetime(
                            year=row[key].year,
                            month=row[key].month,
                            day=row[key].day)
                    elif type(row[key]) == datetime.datetime:
                        r = row[key]
                    record[key] = r.isoformat()
                elif prop.type[1] == "string":
                    record[key] = str(row[key])
                elif prop.type[1] == "number":
                    record[key] = Decimal(row[key])
                elif prop.type[1] == "integer":
                    record[key] = int(row[key])
                else:
                    record[key] = row[key]

            # Replace all Decimal(0.0) values with None.
            def replace_decimal_zero_with_none(obj):
                if isinstance(obj, dict):
                    return {k: replace_decimal_zero_with_none(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [replace_decimal_zero_with_none(elem) for elem in obj]
                elif obj == Decimal(0.0):
                    return None
                else:
                    return obj

            # Replace all Decimal(0.0) values to null.
            record = replace_decimal_zero_with_none(record)            

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
