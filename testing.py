import os, json

import pytest
from unittest.mock import MagicMock

from google.cloud import bigquery

from ingestion_wizard import IngestionWizard


# Note: this suite of tests merely serves to demonstrate the kind of unit tests that one
# would use to validate the functionality of this product. It is by no means a complete
# suite of tests, it just illustrates the most common types of tests one would implement.


def write_sample_data(all_sample_data, test_data_dir):
    """Write all sample data to newline-delimited JSON files."""
    os.makedirs(test_data_dir, exist_ok=True)
    i = 1
    for sample_data in all_sample_data:
        with open(f'{test_data_dir}/sample_data_{i}.json', 'w') as out_file:
            for record in sample_data:
                json.dump(record, out_file)
                out_file.write('\n')
        i += 1


@pytest.fixture
def mock_bq_client(monkeypatch):
    """Mock version of the BQ client."""

    mock_client = MagicMock(spec=bigquery.Client)
    monkeypatch.setattr('ingestion_wizard.bigquery.Client', lambda *args, **kwargs: mock_client)

    return mock_client


@pytest.fixture
def test_data_dir():
    """The testing datasets are declared as fixtures in this script and
    written to this local directory for the class to use."""

    return 'testing'


@pytest.fixture
def sample_data_1():
    """Sample JSON data to test schema inference and table creation with.
    It aims to trigger all routines of the schema inference algorithm.
    For instance: it has all the possible data types, has lists and
    nested fields (including nested records), as well as various different
    timestamp formats that pendulum should be able to parse. Furthermore,
    not every field is present in every JSON object, as this is how we
    expect the data to come out of the hypothetical API. This is also
    enough data to test the data streaming step with in a meaningful way."""

    return [
        {
            'ts': '2020-06-18T10:44:12',
            'started': {
                'pid': 12345
            }
        },
        {
            'ts': '2020-06-18T10:44:13',
            'logged_in':
                {
                    'username': 'foo',
                    'birthday': '1996-04-10'
                }
        },
        {
            'ts': '2020-06-18T10:44:13',
            'session_ids': [123, 456]
        },
        {
            'ts': '2020-06-18T10:44:12',
            'started': {
                'pid': 23456,
                'session_start': '2020-06-18T10:42:45+0200'
            }
        },
        {
            'ts': '2020-06-18T10:44:13',
            'logged_in': {
                'username': 'bar'
            }
        },
        {
            'ts': '2020-06-18T10:44:13',
            'session_ids': [234, 567]
        },
        {
            'ts': '2020-06-18T10:44:12',
            'floaty': 3.1415,
            'booly': False,
            'started': {
                'pid': 34567
            },
            'logged_in': {
                'username': 'spam',
                'password': 'eggs'
            }
        },
        {
            'ts': '2020-06-18T10:44:13',
            'logged_in': {
                'username': 'hello',
                'password': 'world'
            },
            'session_ids': [345, 678]
        },
        {
            'ts': '2020-06-18T10:44:13',
            'started': {
                'pid': 45678,
                'session_start': '2020-06-18T10:43:28+0200'
            },
            'logged_in':{
                'username': 'admin'
            },
            'session_ids': [456, 789]
        },
        {
            'ts': '2020-06-18T10:44:21',
            'repeated_record_holder': [
                {
                    'nested_repeated_string_1': 'some_text',
                    'nested_repeating_float': 0.1123
                }
            ]
        }
    ]


@pytest.fixture
def sample_data_2():
    """Sample JSON data to test schema extension with. Both scenarios
    are triggered by this data: extending the root schema as well as
    extending the schema of an existing nested object."""

    return [
        {
            'ts': '2020-06-18T10:44:13',
            'new_field': True
        },
        {
            'ts': '2020-06-18T10:44:13',
            'logged_in': {
                'new_nested_field': True
            }
        },
        {
            'ts': '2020-06-18T10:44:21',
            'repeated_record_holder': [
                {
                    'nested_repeated_string_2': 'some_more_text'
                }
            ]
        }
    ]


# Note: the layout of the schemas below is not in line with the BigQuery specifications.
# This is because my application uses a more convenient layout (the standard JSON layout)
# internally. The final schemas are still written to file in the BigQuery-friendly format
# when the _schema_writer_wrapper() method is invoked.

@pytest.fixture
def sample_data_1_schema():
    """The expected (dictionary-based) schema of sample 1."""

    return {
        'ts': {
            'type': 'TIMESTAMP',
            'mode': 'REQUIRED',
            'fields': None
        },
        'started': {
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': {
                'pid': {
                    'type': 'INTEGER',
                    'mode':
                    'NULLABLE',
                    'fields': None
                },
            'session_start': {
                'type': 'TIMESTAMP',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    },
    'logged_in': {
        'type': 'RECORD',
        'mode': 'NULLABLE',
        'fields': {
            'username': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            },
            'birthday': {
                'type': 'TIMESTAMP',
                'mode': 'NULLABLE',
                'fields': None
            },
            'password': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    },
    'session_ids': {
        'type': 'INTEGER',
        'mode': 'REPEATED',
        'fields': None
    },
    'floaty': {
        'type': 'FLOAT',
        'mode': 'NULLABLE',
        'fields': None
    },
    'booly': {
        'type': 'BOOLEAN',
        'mode': 'NULLABLE',
        'fields': None
    },
    'repeated_record_holder': {
        'type': 'RECORD',
        'mode': 'REPEATED',
        'fields': {
            'nested_repeated_string_1': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            },
            'nested_repeating_float': {
                'type': 'FLOAT',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    }
}


@pytest.fixture
def sample_data_2_schema():
    """The expected (dictionary-based) schema of sample 2."""

    return {
        'ts': {
            'type': 'TIMESTAMP',
            'mode': 'REQUIRED',
            'fields': None
        },
        'new_field': {
            'type': 'BOOLEAN',
            'mode': 'NULLABLE',
            'fields': None
        },
        'logged_in': {
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': {
                'new_nested_field': {
                    'type': 'BOOLEAN',
                    'mode': 'NULLABLE',
                    'fields': None
                }
            }
        },
        'repeated_record_holder': {
            'type': 'RECORD',
            'mode': 'REPEATED',
            'fields': {
                'nested_repeated_string_2': {
                    'type': 'STRING',
                    'mode': 'NULLABLE',
                    'fields': None
                }
            }
        }
    }


@pytest.fixture
def sample_data_all_schema():
    """The expected (dictionary-based) schema of all sample
    data combined."""

    return {
        'ts': {
            'type': 'TIMESTAMP',
            'mode': 'REQUIRED',
            'fields': None
        },
        'new_field': {
            'type': 'BOOLEAN',
            'mode': 'NULLABLE',
            'fields': None
        },
        'started': {
            'type': 'RECORD',
            'mode': 'NULLABLE',
            'fields': {
                'pid': {
                    'type': 'INTEGER',
                    'mode':
                    'NULLABLE',
                    'fields': None
                },
            'session_start': {
                'type': 'TIMESTAMP',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    },
    'logged_in': {
        'type': 'RECORD',
        'mode': 'NULLABLE',
        'fields': {
            'new_nested_field': {
                'type': 'BOOLEAN',
                'mode': 'NULLABLE',
                'fields': None
            },
            'username': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            },
            'birthday': {
                'type': 'TIMESTAMP',
                'mode': 'NULLABLE',
                'fields': None
            },
            'password': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    },
    'session_ids': {
        'type': 'INTEGER',
        'mode': 'REPEATED',
        'fields': None
    },
    'floaty': {
        'type': 'FLOAT',
        'mode': 'NULLABLE',
        'fields': None
    },
    'booly': {
        'type': 'BOOLEAN',
        'mode': 'NULLABLE',
        'fields': None
    },
    'repeated_record_holder': {
        'type': 'RECORD',
        'mode': 'REPEATED',
        'fields': {
            'nested_repeated_string_2': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            },
            'nested_repeated_string_1': {
                'type': 'STRING',
                'mode': 'NULLABLE',
                'fields': None
            },
            'nested_repeating_float': {
                'type': 'FLOAT',
                'mode': 'NULLABLE',
                'fields': None
            }
        }
    }
}


@pytest.fixture
def all_sample_data(sample_data_1, sample_data_2):
    """Return a list of all sample data to test fetching multiple
    files with."""

    return [sample_data_1, sample_data_2]


# Perform a couple of tests regarding class initialisation and
# optional / compulsory argument configurations

def test_initialization_with_missing_all_args():
    """Test that the intended ValueError is raised when all
    arguments are missing."""

    with pytest.raises(ValueError):
        IngestionWizard()


def test_initialization_with_missing_gcp_project_args():
    """Test that the intended ValueError is raised when the
    GCP project argument is missing."""

    with pytest.raises(ValueError):
        IngestionWizard(
            gcs_bucket_id='test_bucket',
            bq_dataset_id='test_dataset',
            bq_table_id='test_table'
        )


def test_initialization_with_missing_gcs_args():
    """Test that the intended ValueError is raised when the
    GCS bucket argument is missing without having disabled GCS."""

    with pytest.raises(ValueError):
        IngestionWizard(
            gcp_project_id='test_project',
            bq_dataset_id='test_dataset',
            bq_table_id='test_table'
        )


def test_initialization_with_missing_bq_args():
    """Test that the intended ValueError is raised when the
    BQ arguments are missing without having disabled BQ."""

    with pytest.raises(ValueError):
        IngestionWizard(
            gcp_project_id='test_project',
            gcs_bucket_id='test_bucket'
        )


def test_initialization_with_all_args():
    """Test that the class can be initialised when all required
    arguments are present, and that the values are found in the
    expected class variables."""

    wiz = IngestionWizard(
        gcp_project_id='test_project',
        gcs_bucket_id='test_bucket',
        bq_dataset_id='test_dataset',
        bq_table_id='test_table'
    )

    assert wiz.gcp_project_id == 'test_project'
    assert wiz.gcs_bucket_id == 'test_bucket'
    assert wiz.bq_dataset_id == 'test_dataset'
    assert wiz.bq_table_id == 'test_table'


# A few tests on various private methods of the class

def test_fetch_data_local(all_sample_data, test_data_dir):
    """Test fetching a single file from a local directory,
    and whether the fetched data equals the test data."""

    write_sample_data(all_sample_data, test_data_dir)

    wiz = IngestionWizard(
        data_dir=test_data_dir,
        disable_gcs=True,
        disable_bq=True
    )
    wiz._fetch_data_local()

    # Assert that the data was loaded correctly
    assert wiz.data == all_sample_data


def test_infer_schema_single_sample(sample_data_1, sample_data_1_schema):
    """Test that the schema of a single sample dataset is
    inferred correctly."""

    wiz = IngestionWizard(
        disable_gcs=True,
        disable_bq=True
    )
    wiz.data = [sample_data_1]
    wiz._infer_schema_wrapper()

    assert wiz.schema_data == sample_data_1_schema


def test_infer_schema_multiple_samples(all_sample_data, sample_data_all_schema):
    """Test that the schema of multiple sample datasets is
    inferred correctly."""

    wiz = IngestionWizard(
        disable_gcs=True,
        disable_bq=True
    )
    wiz.data = all_sample_data
    wiz._infer_schema_wrapper()

    assert wiz.schema_data == sample_data_all_schema


def test_create_table(mock_bq_client, sample_data_1_schema):
    """Basic test on the BQ table creation private method."""

    wiz = IngestionWizard(
        gcp_project_id='test_project',
        gcs_bucket_id='test_bucket',
        bq_dataset_id='test_dataset',
        bq_table_id='test_table'
    )
    wiz.bq_client = mock_bq_client
    wiz.full_table_id = 'test_project.test_dataset.test_table'

    wiz.schema_data = sample_data_1_schema

    wiz._create_table()

    # Assert that the create_table method was called with the correct arguments
    mock_bq_client.create_table.assert_called()


# Small test on the only public method of the class

def test_run_with_disabling_bq_single_file(sample_data_1, test_data_dir):
    """Test the public run method with BQ interactions disabled
    and a single source file."""

    write_sample_data([sample_data_1], test_data_dir)

    wiz = IngestionWizard(
        data_dir=test_data_dir,
        disable_gcs=True,
        disable_bq=True
    )
    wiz.run()

    # No assertions in this (simple) test, i.e. it only validates
    # that the process runs all the way without throwing an error


def test_run_with_disabling_bq_multiple_files(all_sample_data, test_data_dir):
    """Test the public run method with BQ interactions disabled
    and multiple source files."""

    write_sample_data(all_sample_data, test_data_dir)

    wiz = IngestionWizard(
        data_dir=test_data_dir,
        disable_gcs=True,
        disable_bq=True
    )
    wiz.run()

    # No assertions in this (simple) test, i.e. it only validates
    # that the process runs all the way without throwing an error
