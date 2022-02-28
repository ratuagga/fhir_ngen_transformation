from google.cloud import bigquery
from utils import archive_processed_file


def run_load_to_bq(bucket, csv_file_name, creation_date, extension):
	"""
	This function loads data fro the given csv file to an existing
	table in BigQuery.
	Args:
		bucket:  	   Bucket where th csv file is located in.
		csv_file_name: CSV file that will provide the input data.
		creation_date: Date File was created.
		extension:	   File extension that triggered the event.
	Returns:
		Job status; the output is data loaded to the BigQuery Table.
	"""

	# Initialize client to authenticate the BigQuery API
	client = bigquery.Client()

	# BigQuery Table_id in which data needs to be inserted.
	table_id = "test_csv_load_dataset." + csv_file_name.split('.')[0].split('/')[1]

	# Job Configuration:
	# 1. skip_leading_rows: 	To skip the row with Field Names
	# 2. create_disposition: 	Never Create a Table at destination.
	# 3. write_disposition:		Append the existing table.
	# 4. autodetect:			Do not autodetect schema from input data.
	# 5. schema_update_options: Allow nullable fields to be added in the destination
	#                           table. (To prevent data loss. Can be removed.)
	# 6. source_format:			CSV for our case. It is default value so, optional to keep.
	job_config = bigquery.LoadJobConfig(
		skip_leading_rows=1,
		create_disposition="CREATE_NEVER",
		write_disposition="WRITE_APPEND",
		autodetect=False,
		schema_update_options="ALLOW_FIELD_ADDITION",
		# The source format defaults to CSV, so the line below is optional.
		source_format=bigquery.SourceFormat.CSV
	)
	uri = "gs://" + bucket + "/" + csv_file_name

	load_job = client.load_table_from_uri(
		source_uris=uri,
		destination=table_id,
		job_config=job_config
	)  # Make an API request.

	load_job.result()  # Waits for the job to complete.

	destination_table = client.get_table(table_id)  # Make an API request.
	print("Loaded {} rows.".format(destination_table.num_rows))

	if destination_table.num_rows != 0:
		archive_processed_file(
			source_bucket=bucket,
			file_name=csv_file_name,
			creation_date=creation_date,
			file_type=extension)