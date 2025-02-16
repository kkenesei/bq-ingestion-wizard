import json
import os
import time

import pendulum

from google.cloud import bigquery
from google.cloud import storage
from google.cloud.exceptions import NotFound


class IngestionWizard(object):
    """
    Class to hold all GCS-to-BigQuery ingestion data and functionality.

    Args:
        data_dir (str): GCS (or local, relative) data directory name
        api_tz (str): the pendulum-compatible timezone specification of the API output
        gcp_projectid (str): GCP project ID (for both GCS and BigQuery)
        gcs_bucketid (str): GCS bucket ID where the source data is stored
        bq_datasetid (str): BigQuery dataset ID of the target table
        bq_tableid (str): BigQuery table ID of the target table
        disable_gcs (bool): use this to disable GCS interactions (for debugging purposed)
        disable_bq (bool): use this to disable BQ interactions (for debugging purposed)

    Usage:
        Instantiate with one of the two patterns below.
        Pattern 1 (regular operation):
            Provide all GCP arguments: gcp_projectid, gcs_bucketid, bq_datasetid, bq_tableid.
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
    """

    def __init__(
        self,
        data_dir='data',
        api_tz='Europe/Amsterdam',
        gcp_projectid=None,
        gcs_bucketid=None,
        bq_datasetid=None,
        bq_tableid=None,
        disable_gcs=False,
        disable_bq=False
    ):

        # Detect accidental omission of GCP arguments
        if (
            (not disable_gcs and not (gcp_projectid and gcs_bucketid))
            or (not disable_bq and not (gcp_projectid and bq_datasetid and bq_tableid))
            or (not (disable_gcs and disable_bq) and not
                (gcp_projectid and gcs_bucketid and bq_datasetid and bq_tableid))
        ):
            raise ValueError('One or more GCP arguments missing')

        # Initialise the non-GCP class variables
        self.disable_gcs, self.disable_bq = disable_gcs, disable_bq
        self.data_dir = data_dir
        self.api_tz = api_tz
        self.data, self.schema_data = None, None

        # Supported datatypes
        self.bq_schema_mapping = {
            str: 'STRING',
            float: 'FLOAT',
            int: 'INTEGER',
            bool: 'BOOLEAN',
            type(pendulum.now()): 'TIMESTAMP'
        }

        # Initialise the GCP class variables
        self.gcp_projectid = gcp_projectid
        self.gcs_bucketid = gcs_bucketid
        self.bq_datasetid = bq_datasetid
        self.bq_tableid = bq_tableid

        # Initialise the GCP clients
        if not disable_gcs: self.gcs_client = storage.Client(self.gcp_projectid)
        if not disable_bq: self.bq_client = bigquery.Client(self.gcp_projectid)

    def _init_table(self):
        self.full_tableid = f'{self.gcp_projectid}.{self.bq_datasetid}.{self.bq_tableid}'
        self.table, self.schema_bq = None, None
        try:
            self.table = self.bq_client.get_table(self.full_tableid)
            print('Target table found in BigQuery')
            # Save the existing table schema in a class variable
            self.schema_bq = self._schema_bq_to_dict(self.table.schema)
        except NotFound:
            print('Target table not yet found in BigQuery')

    def _fetch_data(self):
        """Private class method to fetch data from GCS that simulates the output of
        an API ingestion process. Reads all JSON files found in the data directory
        of the source GCS bucket."""
        # List all files in the bucket
        blobs = self.gcs_client.list_blobs(self.gcs_bucketid)
        # Read the GCS data that simulates the output of an ingestion run
        # by filtering on the expected path and file type
        files, data = [b for b in blobs if b.name.startswith(self.data_dir) and b.name.endswith('.json')], []
        for file in files:
            with file.open('r') as in_file:
                # Keep the files separated in the list to avoid accidentally
                # creating 10Mb+ imports that BQ might complain about
                data += [[json.loads(record) for record in in_file]]
        self.data = data
        print('Finished fetching JSON data from GCS')

    def _fetch_data_local(self):
        """Private class method to fetch data that simulates the output of an
        API ingestion process from a local directory. Reads all JSON files found
        in the data directory of the local file system (relative file path)."""
        data = []
        for file in os.listdir(self.data_dir):
            if file.endswith('.json'):
                with open(f'{self.data_dir}/{file}') as in_file:
                    data += [[json.loads(record) for record in in_file]]
        self.data = data
        print('Finished fetching JSON data from local directory')

    def _determine_schema(self, record, schema):
        """Private class method to determine the schema based on the data. The
        recursive implementation ensures that nested fields are handled correctly."""
        for name, value in record.items():
            # Field already in schema? Skip.
            if name in schema and not isinstance(value, dict): continue
            # Set the default mode of all fields
            mode = 'NULLABLE'
            # Detect fields that are repeated and take a sample value
            if isinstance(value, list):
                value = value[0]
                mode = 'REPEATED'
            # Detect new nested fields and go a level deeper in the recursion
            # to determine the nested schema
            if isinstance(value, dict):
                schema[name] = {
                    'type': 'RECORD',
                    'mode': mode,
                    'fields': self._determine_schema(value, schema.get(name, {}).get('fields', {})),
                }
            # All other new fields are simply mapped to BigQuery data types based
            # on their Python data types. For timestamps we utilise pendulum, as it
            # offers a convenient method to detect common timestamp formats.
            else:
                if isinstance(value, str):
                    try: value = pendulum.parse(value)
                    except: pass
                schema[name] = {
                    'type': self.bq_schema_mapping[type(value)],
                    'mode': mode,
                    'fields': None
                }
        return schema

    def _determine_schema_wrapper(self):
        """Private class method to determine the schema based on the data.
        This is a wrapper for the recursive _determine_schema method."""
        # Set the ts field to required, it is assumed to always be generated
        schema = {
            'ts': {
                'type': 'TIMESTAMP',
                'mode': 'REQUIRED',
                'fields': None
            }
        }
        for record in [record for file in self.data for record in file]:
            schema = self._determine_schema(record, schema)
        self.schema_data = schema
        print('Finished determining schema of JSON data')

    def _ts_format(self, record, schema):
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. Recursion also necessary here
        to handle timestamps embedded in nested fields."""
        for name, value in record.items():
            data_type = schema[name]['type']
            # Arrived at nested field: go one level deeper in the recursion
            if data_type == 'RECORD':
                record[name] = self._ts_format(value, schema[name]['fields'])
            # Timestamp encountered: overwrite value with reformatted timestamp.
            # Parser is timezone-aware (assume API uses specific timezone)
            elif data_type == 'TIMESTAMP':
                value = pendulum.parse(value, tz=self.api_tz)
                record[name] = value.to_datetime_string()
        return record

    def _ts_format_wrapper(self):
        """Private class method to format the timestamp values in the data
        in the way that BigQuery expects them. This is a wrapper for the
        recursive _ts_format method."""
        self.data = [[self._ts_format(record, self.schema_data) for record in file] for file in self.data]
        print('Finished formatting timestamp values')

    def _schema_dict_to_bq(self, schema):
        """Private class method to cast a Python-native schema dictionary
        as a list of BigQuery SchemaField objects that can be passed to the
        table directly."""
        return [bigquery.SchemaField(
            name=field,
            field_type=value['type'],
            mode=value['mode'],
            # Recursive implementation to handle nested fields correctly
            fields=self._schema_dict_to_bq(value.get('fields'))
        ) for field, value in schema.items()] if schema else None

    def _schema_bq_to_dict(self, schema):
        """Private class method to cast a list of BigQuery SchemaField
        objects to a Python-native schema dictionary so that it can be
        worked with easier."""
        return {field.name: {
            'type': field.field_type,
            'mode': field.mode,
            # Recursive implementation to handle nested fields correctly
            'fields': self._schema_bq_to_dict(field.fields)
        } for field in schema} if schema else None

    def _merge_schemas(self, schema_data, schema_bq):
        """Private class method to merge two Python-native schema dictionaries.
        schema_data is the detected schema of the data, schema_bq is the schema
        of the existing table. The existing schema is extended with new fields,
        existing fields are not touched. The recursive implementation ensures
        that the schemas of nested fields are also merged properly."""
        # The merge operation itself is just a dictionary union
        merged = schema_data | schema_bq
        # Check for any nested fields in the data schema
        for field, config in schema_data.items():
            # When a nested field is encountered, go one level deeper in the recursion
            if config.get('fields') and schema_bq.get(field):
                merged[field]['fields'] = self._merge_schemas(config['fields'], schema_bq[field]['fields'])
        return merged

    def _merge_schemas_wrapper(self):
        """Private class method to merge two Python-native schema dictionaries.
        This is a wrapper for the recursive _merge_schemas method."""
        self.schema_merged = self._merge_schemas(self.schema_data, self.schema_bq)

    def _create_table(self):
        """Private class method to create the target BigQuery table based on the
        schema of the data"""
        # by the ingestion process
        self.bq_client.create_table(bigquery.Table(self.full_tableid, self._schema_dict_to_bq(self.schema_data)))
        print('Target table has been created at {}'.format(self.full_tableid))

    def _extend_table_schema(self):
        """Private class method to extend the schema of the target BigQuery table
        based on the schema that resulted from extending the existing schema with
        new fields encountered in the data."""
        if self.schema_data != self.schema_bq:
            self._merge_schemas_wrapper()
            self.table.schema = self._schema_dict_to_bq(self.schema_merged)
            self.bq_client.update_table(self.table, ['schema'])
            print('Schema of target table has been extended')
        else: print('Schemas of data and target table already match')

    def _stream_data_to_table(self):
        """Private class method to stream the data to the target BigQuery table.
        This step concludes the ingestion process."""
        for file in self.data:
            # No error is raised when it is the insertion of individual rows
            # that fails, not the BQ job as a whole. The errors are accumulated
            # in a list, and we raise an error manually if it is not empty.
            errors = self.bq_client.insert_rows_json(self.full_tableid, file)
            if errors: raise Exception('Error(s) occurred while inserting rows:\n{}'.format(errors))
        print('Finished streaming data to target table')

    def run(self):
        """Public orchestrator function to execute the ingestion process.
        Invoke this method after instantiating the class to perform the ingestion.
        When GCS interactions are disabled, the data is imported from a
        local directory and no BigQuery interactions are performed.
        When BQ interactions are disabled, the BigQuery steps are skipped."""

        if not self.disable_gcs: self._fetch_data()
        else: self._fetch_data_local()

        if not self.disable_bq: self._init_table()

        self._determine_schema_wrapper()
        self._ts_format_wrapper()

        if not self.disable_bq:
            if self.table: self._extend_table_schema()
            else: self._create_table()
            self._stream_data_to_table()
        else: print('Skipping BigQuery operations')

        print('All steps of ingestion process have completed. Cheers!')
