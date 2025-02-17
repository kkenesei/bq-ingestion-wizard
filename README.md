# Newline-delimited JSON to BigQuery ingestion wizard

Implements the IngestionWizard class which performs schema inference on newline-delimited
JSON data and creates / extends the target a BigQuery table as appropriate. It also streams
the data into the newly-created or extended target table. The inferred schema of the data and
(if applicable) the schema resulting from merging it with the schema of an existing target
table is always written to file for future reference. All JSON data types are supported, as
well as timestamps. JSON arrays, and any depth of object nesting are also supported.

The implementation is designed to run as a cloud app in GCP Cloud Run, but it can also be
run manually in a local development environment.

## Usage

### Fully local
1. Create a Python virtual environment (preferably `>=3.12`, for optimal type hints)
2. Install the dependencies via `requirements.txt`
3. Import `IngestionWizard` and instantiate it with the desired configuration of arguments
4. Invoke the `.run()` method of the wizard

### Local web app
1. Create a Python virtual environment (preferably `>=3.12`, for optimal type hints)
2. Install the dependencies via `requirements.txt`
3. Run `main.py` to host locally as web app via `Flask`
4. Send a `POST` request to the `/bel_mij` endpoint with the desired configuration of arguments

### Cloud Run
Ensure that the Cloud Run instance and the GCS and BigQuery resources are in the same GCP project
(or use cross-project IAM policies).
1. Fork this repository and connect it to a Cloud Run instance
2. Send a `POST` request to the `/bel_mij` endpoint with the desired configuration of arguments

#### Note
For a quick validation of the core functionality, run `pytest testing.py`. This will generate
some example data as well as output an inferred schema. For more in-depth analysis please
instantiate the class (with GCP features disabled, for convenience) and step through the
private method invocations used by `.run()`.

_Please refer to the extensive inline documentation for more information_
