import sys

from ingestion_wizard import IngestionWizard

if __name__ == '__main__':

    if len(sys.argv) != 5:
        raise ValueError('Expected 4 arguments: <gcp_projectid> <gcs_bucketid> <bq_datasetid> <bq_tableid>')

    wiz = IngestionWizard(
        gcp_projectid=sys.argv[3],
        gcs_bucketid=sys.argv[4],
        bq_datasetid=sys.argv[5],
        bq_tableid=sys.argv[6],
        disable_gcs=True
    )
    wiz.run()
