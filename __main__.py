import sys

from ingestion_wizard import IngestionWizard

if __name__ == '__main__':

    if len(sys.argv) != 7:
        raise ValueError(
            'Expected arguments: <data_dir> <api_tz> <gcp_projectid> <gcs_bucketid> <bq_datasetid> <bq_tableid>')

    handler = IngestionWizard(
        data_dir=sys.argv[1],
        api_tz=sys.argv[2],
        gcp_projectid=sys.argv[3],
        gcs_bucketid=sys.argv[4],
        bq_datasetid=sys.argv[5],
        bq_tableid=sys.argv[6],
        disable_gcs=True
    )

    handler.run()
