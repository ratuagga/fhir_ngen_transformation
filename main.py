from dataflow_pipeline import run


def cloud_function_run(event, context):
    """The main function which creates the pipeline and runs it."""
    input_file = event['name']
    project_id = 'vocal-collector-334618'
    job_name = input_file + "_" + event['updated']
    run(input_file, project_id, job_name)