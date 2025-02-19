import os, json
from typing import Dict, List, Optional, Union

import pendulum

from google.api_core.page_iterator import HTTPIterator
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


# Declare types for the recursive dictionaries used by this class
RecursiveDictValue = Union[str, int, float, 'RecursiveDict', List['RecursiveDictValue']]
RecursiveDict = Dict[str, RecursiveDictValue]


def reformat_timestamp(value: str, tz: str) -> str:
    """Function to reformat any pendulum-compatible timestamp
    string into a BigQuery-friendly format."""

    return pendulum.parse(value, tz=tz).to_datetime_string()


class IngestionWizard:
    """
    Class to hold all GCS-to-BigQuery ingestion data and functionality.
    Performs schema inference on the data and creates / updates the target
    BigQuery table as needed, as well as streams the data into the target table.
    The inferred schema of the data and (if applicable) the schema resulting
    from merging it with the schema of an existing target table is always
    written to file. All JSON data types are supported, as well as timestamps.
    JSON arrays, and any depth of object nesting are also supported.

    Args:
        data_dir (str, default: data): GCS (or local, relative) data input directory name
        schema_dir (str, default: data): GCS (or local, relative) schema output directory name
        api_tz (str, default: Europe/Amsterdam): the timezone specification of the API output
        gcp_project_id (str): GCP project ID (for both GCS and BigQuery)
        gcs_bucket_id (str): GCS bucket ID where the source data is stored
        bq_dataset_id (str): BigQuery dataset ID of the target table
        bq_table_id (str): BigQuery table ID of the target table
        disable_gcs (bool, default: False): use this to disable GCS interactions (when debugging)
        disable_bq (bool, default: False): use this to disable BQ interactions (when debugging)

    Usage:
        Instantiate with one of the two patterns below.
        Pattern 1 (regular operation):
            Provide all GCP arguments: gcp_project_id, gcs_bucket_id, bq_dataset_id, bq_table_id.
            There are no defaults for these arguments.
        Pattern 2 (GCP operations disabled):
            Set disable_gcp or disable_bq (or both) to True. Depending on which feature set
            is disabled, one may omit some or all of the GCP arguments. Note: disabling GCS
            results in the Wizard looking for the source data and outputting the schema files
            in a local relative folder (this also uses the data_dir and schema_dir arguments).
            However, there is no local alternative for the BigQuery steps; these are skipped.
        Once instantiated, invoke .run() to start the ingestion process.

    Example usage:
        wiz = IngestionWizard(
            gcp_project_id='my_project',
            gcs_bucket_id='my_bucket',
            bq_dataset_id='my_dataset',
            bq_table_id='my_table'
        )
        wiz.run()
    """

    def __init__(
        self,
        data_dir: Optional[str] = None,
        schema_dir: Optional[str] = None,
        api_tz: Optional[str] = None,
        gcp_project_id: Optional[str] = None,
        gcs_bucket_id: Optional[str] = None,
        bq_dataset_id: Optional[str] = None,
        bq_table_id: Optional[str] = None,
        disable_gcs: Optional[bool] = None,
        disable_bq: Optional[bool] = None
    ):

        # Detect accidental omission of GCP arguments
        if not ((disable_gcs and disable_bq) or isinstance(gcp_project_id, str)):
            raise ValueError('Please provide GCP project ID or turn off all GCP interactions')
        if not (disable_gcs or isinstance(gcs_bucket_id, str)):
            raise ValueError('Please provide GCS bucket ID or disable GCS interactions')
        if not (disable_bq or (isinstance(bq_dataset_id, str) and isinstance(bq_table_id, str))):
            raise ValueError('Please provide the BQ parameters or disable BQ interactions')

        # Initialise the general class variables (defaults are also applied here)
        self.disable_gcs, self.disable_bq = disable_gcs or False, disable_bq or False
        self.data_dir, self.schema_dir = data_dir or 'data', schema_dir or 'schemas'
        self.api_tz = api_tz or 'Europe/Amsterdam'
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

        # Initialise the GCP-related class variables
        self.gcp_project_id = gcp_project_id
        self.gcs_bucket_id = gcs_bucket_id
        self.bq_dataset_id = bq_dataset_id
        self.bq_table_id = bq_table_id
        self.full_table_id: Optional[str] = None
        self.table: Optional[bigquery.table.Table] = None
        self.gcs_client: Optional[storage.Client] = None
        self.bq_client: Optional[bigquery.Client] = None


    def _init_clients(self) -> None:
        """Private class method to initialise the GCP clients."""

        if not self.disable_gcs: self.gcs_client = storage.Client(self.gcp_project_id)
        if not self.disable_bq: self.bq_client = bigquery.Client(self.gcp_project_id)


    def _init_table(self) -> None:
        """Private class method to fetch the target table's metadata from
        BigQuery and in doing so also establish whether the table already exists
        (no dedicated method in SDK to check existence of table)."""

        if not self.disable_bq:

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


    def _fetch_data_gcs(self) -> None:
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


    def _fetch_data(self) -> None:
        """Private class method to decide whether to invoke the local or
        the GCS fetch data method."""

        if not self.disable_gcs: self._fetch_data_gcs()
        else: self._fetch_data_local()


    def _infer_schema(self, record: RecursiveDict, schema: RecursiveDict) -> RecursiveDict:
        """Private class method to infer the schema of the data. The recursive
        implementation ensures that nested fields are processed correctly."""

        for name, value in record.items():

            # Missing value or empty list: skip, unsuitable for type inference
            if not value or (isinstance(value, list) and not len(value)): continue

            # Non-dict field already in schema: skip, we do not need to check again
            field_schema = schema.get(name)
            if field_schema:
                if field_schema['type'] != 'RECORD': continue

            # If a nested field is encountered, go one level deeper in the recursion
            if isinstance(value, dict):
                schema[name] = {
                    'type': 'RECORD',
                    'mode': 'NULLABLE',
                    'fields': self._infer_schema(value, schema.get(name, {}).get('fields', {})),
                }

            # If a nested-repeated field is detected, go one level deeper in the recursion
            # for each instance of the nested field and add any newly-discovered fields
            elif isinstance(value, list) and isinstance(value[0], dict):
                fields = schema.get(name, {}).get('fields', {})
                for item in value:
                    fields = fields | self._infer_schema(item, schema.get(name, {}).get('fields', {}))
                schema[name] = {
                    'type': 'RECORD',
                    'mode': 'REPEATED',
                    'fields': fields
                }

            # Leaf node fields are simply mapped to BigQuery data types based on their
            # Python data types. For timestamps, we utilise pendulum, as it offers a
            # convenient method to detect common timestamp formats.
            else:

                mode: str = 'NULLABLE'
                # Detect fields that are repeated and take a sample value for type inference
                if isinstance(value, list):
                    value = value[0]
                    mode = 'REPEATED'

                # Temporary value storage that allows pendulum objects
                temp_value: RecursiveDictValue | pendulum.DateTime = value

                # Timestamps are "hidden" in JSON string fields
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


    def _schema_writer_wrapper(self, schema: RecursiveDict, filename: str) -> None:
        """Private class method to write a BigQuery-friendly version of
        a Python-native schema to file. Write to local directory when
        GCS interactions are disabled, otherwise write to GCS."""

        path: str = f'{self.schema_dir}/{filename}'
        schema_json: str = json.dumps(self._schema_writer(schema), indent=4)

        # Write to GCS if GCS interactions are not disabled
        if not self.disable_gcs:
            with self.gcs_client.bucket(self.gcs_bucket_id).blob(path).open('w') as out_file:
                out_file.write(schema_json)

        # Otherwise write to local directory
        else:
            os.makedirs(self.schema_dir, exist_ok=True)
            with open(path, 'w') as out_file:
                out_file.write(schema_json)


    def _ts_format(self, record: RecursiveDict, schema: RecursiveDict) -> RecursiveDict:
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. Recursion also necessary here
        to handle timestamps embedded in nested fields."""

        for name, value in record.items():

            if not value or (isinstance(value, list) and not len(value)): continue

            # Arrived at nested field: go one level deeper in the recursion
            elif schema[name]['type'] == 'RECORD':
                if schema[name]['mode'] == 'REPEATED':
                    record[name] = [self._ts_format(item, schema[name]['fields']) for item in value]
                else:
                    record[name] = self._ts_format(value, schema[name]['fields'])

            # Timestamp encountered: overwrite value with reformatted timestamp.
            # Parser is timezone-aware (assume API uses specific timezone)
            elif schema[name]['type'] == 'TIMESTAMP':
                if schema[name]['mode'] == 'REPEATED':
                    record[name] = [reformat_timestamp(item, self.api_tz) for item in value]
                else:
                    record[name] = reformat_timestamp(value, self.api_tz)

        return record


    def _ts_format_wrapper(self) -> None:
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. This is a wrapper for the
        recursive _ts_format method."""

        self.data = [[self._ts_format(record, self.schema_data) for record in file] for file in self.data]

        print('Finished formatting the timestamp values')


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


    def _create_or_extend_table(self) -> None:
        """Private class method to decide whether to create a new BigQuery table
        or to extend an existing one."""

        if not self.disable_bq:

            if self.table: self._extend_table_schema()
            else: self._create_table()


    def _stream_data_to_table(self) -> None:
        """Private class method to stream the data to the target BigQuery table.
        This step concludes the ingestion process."""

        if not self.disable_bq:

            for file in self.data:
                # No error is raised when it is the insertion of individual rows
                # that fails, not the BQ job as a whole. The errors are accumulated
                # in a list, and we raise an error manually if it is not empty.
                errors = self.bq_client.insert_rows_json(self.full_table_id, file)
                if errors: raise Exception('Error(s) occurred while inserting rows:\n{}'.format(errors))

            print('Finished streaming data to target table')


    def run(self) -> None:
        """Public orchestrator function to execute the ingestion process.
        Invoke this method after instantiating the class to perform ingestion."""

        if self.disable_gcs: print('Skipping all GCS interactions; using local data.')
        if self.disable_bq: print('Skipping all BQ operations.')

        self._init_clients()
        self._fetch_data()
        self._init_table()
        self._infer_schema_wrapper()
        self._ts_format_wrapper()
        self._create_or_extend_table()
        self._stream_data_to_table()

        print('All steps of ingestion process have completed. Graag gedaan.')
