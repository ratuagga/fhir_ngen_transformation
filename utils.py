"""Common Helper Functions."""
from google.cloud import storage


def check_file_exist_in_gcs(target_bucket, full_file_path) -> bool:
    """Check if a file exist in GCS Bucket."""
    storage_client = storage.Client()
    operating_bucket = storage_client.bucket(target_bucket)
    return storage.Blob(name=full_file_path, bucket=operating_bucket).exists(storage_client)


def archive_processed_file(source_bucket, file_name, creation_date, file_type):
    """Move processed file to Archive Bucket."""
    # Create a gcs storage client.
    storage_client = storage.Client()

    operating_bucket = storage_client.bucket(source_bucket)
    operating_file = operating_bucket.blob(file_name)
    destination_bucket = storage_client.bucket(
        "test_bucket_for_cloud_trigger_archive_files")
    output_file = creation_date + "/" + file_type + "/" + file_name.split("/")[1]

    try:
        blob_copy = operating_bucket.copy_blob(operating_file, destination_bucket, output_file)
    except:
        # Change to log the error in a file.
        raise Exception(f"Encountered an error while archiving {file_name}.")
    else:
        operating_bucket.delete_blob(file_name)
        print(f"Moved {file_name.split('/')[1]} to test_bucket_for_cloud_trigger_archive_files/"
              f"{creation_date}/{file_type} folder.")
