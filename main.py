import os

from flask import Flask, request

from ingestion_wizard import IngestionWizard

app = Flask(__name__)

@app.route('/bel_mij', methods=['POST'])
def call_the_wizard() -> str:

    wiz = IngestionWizard(
        data_dir=request.args.get('data_dir'),
        api_tz=request.args.get('api_tz'),
        gcp_project_id=request.args.get('gcp_project_id'),
        gcs_bucket_id=request.args.get('gcs_bucket_id'),
        bq_dataset_id=request.args.get('bq_dataset_id'),
        bq_table_id=request.args.get('bq_table_id')
    )
    wiz.run()
    return 'Done! All the right buttons were pressed.'


if __name__ == '__main__':

    app.run(
        debug=True,
        host='0.0.0.0',
        port=int(os.environ.get('PORT', 8080))
    )
