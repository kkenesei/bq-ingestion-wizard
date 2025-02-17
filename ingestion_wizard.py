import json
import os
from typing import Dict, List, Optional, Union

import pendulum

from google.api_core.page_iterator import HTTPIterator
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


# Declare types for the recursive dictionaries used by this class
RecursiveDictValue = Union[str, int, float, 'RecursiveDict', List['RecursiveDictValue']]
RecursiveDict = Dict[str, RecursiveDictValue]


class IngestionWizard:
    """
    Class to hold all GCS-to-BigQuery ingestion data and functionality.
    Performs schema inference on the data and creates / updates the target
    BigQuery table as needed, as well as streams the data into the target table.
    All JSON data types are supported, as well as timestamps. JSON arrays, and
    any depth of object nesting are also supported.

    Args:
        data_dir (str): GCS (or local, relative) data directory name
        api_tz (str): the pendulum-compatible timezone specification of the API output
        gcp_project_id (str): GCP project ID (for both GCS and BigQuery)
        gcs_bucket_id (str): GCS bucket ID where the source data is stored
        bq_dataset_id (str): BigQuery dataset ID of the target table
        bq_table_id (str): BigQuery table ID of the target table
        disable_gcs (bool): use this to disable GCS interactions (for debugging purposed)
        disable_bq (bool): use this to disable BQ interactions (for debugging purposed)

    Usage:
        Instantiate with one of the two patterns below.
        Pattern 1 (regular operation):
            Provide all GCP arguments: gcp_project_id, gcs_bucket_id, bq_dataset_id, bq_table_id.
            There are no defaults for these arguments. Omit disable_gcp or set it to False.
        Pattern 2 (GCP operations disabled):
            Set disable_gcp or disable_bq (or both) to True. Depending on which feature set
            is disabled, one may omit some or all of the GCP arguments. Note: disabling GCS
            results in the Wizard looking for the files in a local relative folder (this
            also uses the data_dir argument). However, there is no local alternative for the
            BigQuery steps, these are simply skipped.
        The arguments data_dir and api_tz are always optional, their default values are
        'dummy_data' and 'Europe/Amsterdam' respectively.
        Once instantiated, invoke .run() to start the ingestion process.
        The inferred (and merged, if present) schemas are always written to JSON
        files in the folder where this script runs.

    Example usage:
        wiz = IngestionWizard(
            gcp_projectid='my_project',
            gcs_bucketid='my_bucket',
            bq_datasetid='my_dataset',
            bq_tableid='my_table'
        )
        wiz.run()
    """

    def __init__(
        self,
        data_dir: Optional[str] = 'data',
        api_tz: Optional[str] = 'Europe/Amsterdam',
        gcp_project_id: Optional[str] = None,
        gcs_bucket_id: Optional[str] = None,
        bq_dataset_id: Optional[str] = None,
        bq_table_id: Optional[str] = None,
        disable_gcs: Optional[bool] = False,
        disable_bq: Optional[bool] = False
    ):

        # Detect accidental omission of GCP arguments
        if (
            (not disable_gcs and not (gcp_project_id and gcs_bucket_id))
            or (not disable_bq and not (gcp_project_id and bq_dataset_id and bq_table_id))
            or (not (disable_gcs and disable_bq) and not
                (gcp_project_id and gcs_bucket_id and bq_dataset_id and bq_table_id))
        ):
            raise ValueError('One or more GCP arguments missing')

        # Initialise the non-GCP class variables
        self.disable_gcs, self.disable_bq = disable_gcs, disable_bq
        self.data_dir = data_dir
        self.api_tz = api_tz
        self.data: Optional[List[List[RecursiveDict]]] = None
        self.schema_data: Optional[RecursiveDict] = None
        self.schema_bq: Optional[RecursiveDict] = None
        self.schema_merged: Optional[RecursiveDict] = None

        # Supported datatypes
        self.bq_schema_mapping: Dict[type, str] = {
            str: 'STRING',
            float: 'FLOAT',
            int: 'INTEGER',
            bool: 'BOOLEAN',
            pendulum.DateTime: 'TIMESTAMP'
        }

        # Initialise the GCP class variables
        self.gcp_project_id = gcp_project_id
        self.gcs_bucket_id = gcs_bucket_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.full_table_id: Optional[str] = None
        self.table: Optional[bigquery.table.Table] = None

        # Initialise the GCP clients
        self.gcs_client: Optional[storage.Client] = None
        self.bq_client: Optional[bigquery.Client] = None
        if not disable_gcs: self.gcs_client = storage.Client(self.gcp_project_id)
        if not disable_bq: self.bq_client = bigquery.Client(self.gcp_project_id)

    def _init_table(self) -> None:
        """Private class method to fetch the target table's metadata from
        BigQuery and in doing so also establish whether the table already exists
        (no dedicated method in SDK to check existence of table)."""

        # Assemble the full table path / id
        self.full_table_id = f'{self.gcp_project_id}.{self.bq_dataset_id}.{self.bq_table_id}'

        # Try pulling the metadata of the table
        try:
            self.table = self.bq_client.get_table(self.full_table_id)
            print('Target table found in BigQuery')
            # Save the existing table schema in a class variable
            self.schema_bq = self._schema_bq_to_dict(self.table.schema)

        except NotFound:
            print('Target table not yet found in BigQuery')

    def _fetch_data(self) -> None:
        """Private class method to fetch data from GCS that simulates the output of
        an API ingestion process. Reads all JSON files found in the data directory
        of the source GCS bucket."""

        # List all files in the bucket
        blobs: HTTPIterator = self.gcs_client.list_blobs(self.gcs_bucket_id)
        files: Optional[List[storage.blob.Blob]] = \
            [b for b in blobs if b.name.startswith(self.data_dir) and b.name.endswith('.json')]

        # Throw an exception if no files were found in the expected place
        if not files: raise FileNotFoundError('No JSON files found in GCS')

        # Read the GCS data that simulates the output of an ingestion run
        # by filtering on the expected path and file type
        all_data: Optional[List[List[RecursiveDict]]] = []
        for file in files:
            with file.open('r') as in_file:
                # Keep the files separated in the list to avoid accidentally
                # creating 10Mb+ imports that BQ might complain about
                current_data: Optional[List[RecursiveDict]] = [json.loads(record) for record in in_file]
                if not current_data: print(f'File {file.name} yielded no data')
                else: all_data += [current_data]

        # Throw an exception if the JSON files yielded no data
        if not all_data: raise Exception('None of the JSON files yielded any data')

        self.data = all_data

        print('Finished fetching JSON data from GCS')

    def _fetch_data_local(self) -> None:
        """Private class method to fetch data that simulates the output of an
        API ingestion process from a local directory. Reads all JSON files found
        in the data directory of the local file system (relative file path)."""

        # List all files in the local directory
        files: List[str] = [file for file in os.listdir(self.data_dir) if file.endswith('.json')]

        # Throw an exception if no files were found in the expected place
        if not files: raise FileNotFoundError('No JSON files found in the local directory')

        # Read the local data that simulates the output of an ingestion run
        all_data: Optional[List[List[RecursiveDict]]] = []
        for file in os.listdir(self.data_dir):
            if file.endswith('.json'):
                with open(f'{self.data_dir}/{file}') as in_file:
                    # Keep the files separated in the list to avoid accidentally
                    # creating 10Mb+ imports that BQ might complain about
                    current_data: Optional[List[RecursiveDict]] = [json.loads(record) for record in in_file]
                    if not current_data: print(f'File {file} yielded no data')
                    else: all_data += [current_data]

        # Throw an exception if the JSON files yielded no data
        if not all_data: raise Exception('None of the JSON files yielded any data')

        self.data = all_data

        print('Finished fetching JSON data from local directory')

    def _infer_schema(self, record: RecursiveDict, schema: RecursiveDict) -> RecursiveDict:
        """Private class method to infer the schema of the data. The recursive
        implementation ensures that nested fields are processed correctly."""

        for name, value in record.items():

            # Field already in schema? Skip.
            if name in schema and not isinstance(value, dict): continue

            # Set the default mode of all fields
            mode: str = 'NULLABLE'

            # If a nested field is detected, go one level deeper in the recursion
            if isinstance(value, dict):
                schema[name] = {
                    'type': 'RECORD',
                    'mode': mode,
                    'fields': self._infer_schema(value, schema.get(name, {}).get('fields', {})),
                }

            else:
                # Detect fields that are repeated and take a sample value for later type inference
                if isinstance(value, list):
                    if len(value):
                        value = value[0]
                        mode = 'REPEATED'
                    # Empty lists are not suitable for type inference
                    else: continue

                # All other new fields are simply mapped to BigQuery data types based
                # on their Python data types. For timestamps, we utilise pendulum, as it
                # offers a convenient method to detect common timestamp formats.

                # Temporary value storage that allows pendulum objects
                temp_value: RecursiveDictValue | pendulum.DateTime = value

                # Timestamps hide in JSON string fields
                if isinstance(value, str):
                    try: temp_value = pendulum.parse(temp_value)
                    except: pass

                schema[name] = {
                    'type': self.bq_schema_mapping[type(temp_value)],
                    'mode': mode,
                    'fields': None
                }

        return schema

    def _infer_schema_wrapper(self) -> None:
        """Private class method to determine the schema based on the data.
        This is a wrapper for the recursive _infer_schema method."""

        # Set the ts field to required, it is assumed to always be generated
        schema: RecursiveDict = {
            'ts': {
                'type': 'TIMESTAMP',
                'mode': 'REQUIRED',
                'fields': None
            }
        }

        # Detect schema extensions (starting with just ts) record by record
        for record in [record for file in self.data for record in file]:
            schema = self._infer_schema(record, schema)

        self.schema_data = schema

        # Write the merged schema to disk in case it needs to be checked
        self._schema_writer_wrapper(self.schema_data, 'inferred_schema.json')

        print('Finished determining schema of JSON data')

    def _ts_format(self, record: RecursiveDict, schema: RecursiveDict) -> RecursiveDict:
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. Recursion also necessary here
        to handle timestamps embedded in nested fields."""

        for name, value in record.items():

            # Arrived at nested field: go one level deeper in the recursion
            if schema[name]['type'] == 'RECORD':
                record[name] = self._ts_format(value, schema[name]['fields'])

            # Timestamp encountered: overwrite value with reformatted timestamp.
            # Parser is timezone-aware (assume API uses specific timezone)
            elif schema[name]['type'] == 'TIMESTAMP':
                record[name] = pendulum.parse(value, tz=self.api_tz).to_datetime_string()

        return record

    def _ts_format_wrapper(self) -> None:
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. This is a wrapper for the
        recursive _ts_format method."""

        self.data = [[self._ts_format(record, self.schema_data) for record in file] for file in self.data]

        print('Finished formatting the timestamp values')

    def _schema_writer(self, schema: RecursiveDict) -> List[RecursiveDict]:
        """Private class method to cast a Python-native schema dictionary
        into the BigQuery-friendly layout so that it can be written to file."""

        schema_out: list = []
        for name, value in schema.items():

            # Non-nested fields
            field: RecursiveDict = {
                'name': name,
                'type': value['type'],
                'mode': value['mode']
            }

            # Recursive implementation to handle nested fields correctly
            if value.get('fields'):
                field['fields'] = self._schema_writer(value['fields'])

            schema_out += [field]

        return schema_out

    def _schema_writer_wrapper(self, schema: RecursiveDict, file_name: str) -> None:
        """Private class method to write a BigQuery-friendly version of
        Python-native schema to file. Write to the script location when
        GCS interactions are disabled, otherwise write to the root of
        the GCS bucket."""

        schema_json: str = json.dumps(self._schema_writer(schema), indent=4)

        # Write to GCS if GCS interactions are not disabled
        if not self.disable_gcs:
            with self.gcs_client.bucket(self.gcs_bucket_id).blob(file_name).open("w") as out_file:
                out_file.write(schema_json)

        # Otherwise write to the script directory
        else:
            with open(file_name, 'w') as out_file:
                out_file.write(schema_json)

    def _schema_dict_to_bq(self, schema: RecursiveDict) -> List[bigquery.SchemaField]:
        """Private class method to cast a Python-native schema dictionary
        as a list of BigQuery SchemaField objects that can be passed to the
        table directly."""

        return [bigquery.SchemaField(
            name=name,
            field_type=value['type'],
            mode=value['mode'],
            # Recursive implementation to handle nested fields correctly
            fields=self._schema_dict_to_bq(value.get('fields'))
        ) for name, value in schema.items()] if schema else None

    def _schema_bq_to_dict(self, schema: List[bigquery.SchemaField]) -> RecursiveDict:
        """Private class method to cast a list of BigQuery SchemaField
        objects to a Python-native schema dictionary so that it can be
        worked with easier."""

        return {field.name: {
            'type': field.field_type,
            'mode': field.mode,
            # Recursive implementation to handle nested fields correctly
            'fields': self._schema_bq_to_dict(field.fields)
        } for field in schema} if schema else None

    def _merge_schemas(self, schema_data: RecursiveDict, schema_bq: RecursiveDict) -> RecursiveDict:
        """Private class method to merge two Python-native schema dictionaries.
        schema_data is the detected schema of the data, schema_bq is the schema
        of the existing table. The existing schema is extended with new fields,
        existing fields are not touched. The recursive implementation ensures
        that the schemas of nested fields are also merged properly."""

        # The merge operation of leaf nodes is just a dictionary union
        merged: RecursiveDict = schema_data | schema_bq

        # Check for any nested fields in the data schema as these need recursion
        for field, config in schema_data.items():
            # When a nested field is encountered, go one level deeper in the recursion
            if config.get('fields') and schema_bq.get(field):
                merged[field]['fields'] = self._merge_schemas(config['fields'], schema_bq[field]['fields'])

        return merged

    def _merge_schemas_wrapper(self) -> None:
        """Private class method to merge two Python-native schema dictionaries.
        This is a wrapper for the recursive _merge_schemas method."""

        self.schema_merged = self._merge_schemas(self.schema_data, self.schema_bq)

        # Write the merged schema to disk in case it needs to be checked
        self._schema_writer_wrapper(self.schema_merged, 'merged_schema.json')

    def _create_table(self) -> None:
        """Private class method to create the target BigQuery table based on the
        schema of the data"""

        self.bq_client.create_table(bigquery.Table(self.full_table_id, self._schema_dict_to_bq(self.schema_data)))

        print('Target table has been created at {}'.format(self.full_table_id))

    def _extend_table_schema(self) -> None:
        """Private class method to extend the schema of the target BigQuery table
        based on the schema that resulted from extending the existing schema with
        new fields encountered in the data."""

        if self.schema_data != self.schema_bq:
            self._merge_schemas_wrapper()
            self.table.schema = self._schema_dict_to_bq(self.schema_merged)
            self.bq_client.update_table(self.table, ['schema'])
            print('Schema of target table has been extended')

        else: print('Schemas of data and target table already match')

    def _stream_data_to_table(self) -> None:
        """Private class method to stream the data to the target BigQuery table.
        This step concludes the ingestion process."""

        for file in self.data:
            # No error is raised when it is the insertion of individual rows
            # that fails, not the BQ job as a whole. The errors are accumulated
            # in a list, and we raise an error manually if it is not empty.
            errors = self.bq_client.insert_rows_json(self.full_table_id, file)
            if errors: raise Exception('Error(s) occurred while inserting rows:\n{}'.format(errors))

        print('Finished streaming data to target table')

    def run(self) -> None:
        """Public orchestrator function to execute the ingestion process.
        Invoke this method after instantiating the class to perform ingestion.
        When GCS interactions are disabled, the data is imported from a
        local directory and no BigQuery interactions are performed.
        When BQ interactions are disabled, the BigQuery steps are skipped."""

        if not self.disable_gcs: self._fetch_data()
        else: self._fetch_data_local()

        if not self.disable_bq: self._init_table()

        self._infer_schema_wrapper()
        self._ts_format_wrapper()

        if not self.disable_bq:
            if self.table: self._extend_table_schema()
            else: self._create_table()
            self._stream_data_to_table()
        else: print('Skipping BigQuery operations')

        print('All steps of ingestion process have completed. Graag gedaan.')
