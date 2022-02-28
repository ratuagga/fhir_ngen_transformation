from fhir_to_omop_main import run_fhir_to_omop
from csv_to_bq_main import run_load_to_bq


def fhir_to_omop_trigger(event, context):
    """This cloud function gets triggered by a csv file creation in Cloud Storage.
       This event runs a script to load the csv file data to an existing BigQuery Table.
    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None;
        Output 'run_fhir_to_omop': Create CSV file in GCS Bucket with OMOP formatted data.
        Output 'run_load_to_bq': Load OMOP CSV data into BigQuery Table and archive processed CSV file.
    """

    event_id = context.event_id
    event_type = context.event_type
    cloud_bucket = event['bucket']
    file_name = event['name']
    input_file_creation_time = event['timeCreated']
    input_file_last_updated = event['updated']
    input_file_creation_date = input_file_creation_time.split('T')[0]
    # Find a way to not hard-code project_id here.
    project_id = 'playground-project-two'

    # Find out the extension of the file.
    # Expected file_name format: 'folder/file_name.ext'
    extension = file_name.split('.')[1]

    if extension == "json":
        # Run FHIR to OMOP mapping function.
        run_fhir_to_omop(bucket=cloud_bucket,
                         project_id=project_id,
                         file_name=file_name,
                         creation_date=input_file_creation_date,
                         extension=extension)

    elif extension == "csv":
        # Run Loading CSV data to BigQuery function.
        run_load_to_bq(bucket=cloud_bucket,
                       csv_file_name=file_name,
                       creation_date=input_file_creation_date,
                       extension=extension)