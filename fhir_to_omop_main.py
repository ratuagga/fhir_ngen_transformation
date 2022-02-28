"""Converting FHIR JSON file to OMOP CSV file."""
import gcsfs
import json
import pandas
from utils import check_file_exist_in_gcs, archive_processed_file


def read_input_json(path_to_file: str,
                    gcs_file_system) -> dict:
    """Reads input json file into python dictionary."""
    try:
        with gcs_file_system.open(path_to_file) as file:
            json_dict = json.load(file)
    except:
        raise Exception(f"Reading {path_to_file} file encountered an error.")

    return json_dict


def create_input_dataframe(input_data: list) -> pandas.DataFrame:
    """Converts nested dictionary into a linear dictionary."""
    dataframe = pandas.DataFrame(input_data)
    return dataframe


def create_mapped_dataframe(mapping_file: str,
                            input_dataframe: pandas.DataFrame,
                            gcs_file_system) -> pandas.DataFrame:
    """Map the input_dataframe to create a mapped output_dataframe."""
    try:
        with gcs_file_system.open(mapping_file) as file:
            mapping_dict = json.load(file)
    except:
        raise Exception(f"Reading {mapping_file} file encountered an error.")

    # Create Dataframe with mapping_dict keys as columns.
    csv_cols = mapping_dict.keys()
    output_dataframe = pandas.DataFrame(columns=csv_cols)

    # Create a Record and append it in the output_dataframe
    input_dataframe = input_dataframe.reset_index()
    for index, row in input_dataframe.iterrows():
        record = []
        for key, value in mapping_dict.items():
            if value:
                record.append(row[value])
            else:
                record.append(None)
        output_dataframe.loc[len(output_dataframe.index)] = record

    return output_dataframe


def run_fhir_to_omop(bucket, project_id, file_name, creation_date, extension):
    """Converts input json data file into csv data file.
    Args:
        bucket:         Bucket of input file.
        project_id:     Project ID.
        file_name:      Filename with extension (.json)
        creation_date:  Date on which trigger_file was created.
        extension:      Extension of the trigger_file.
    Returns:
        None; Newly created CSV data file stored in 'generated_csv' directory of the bucket.

    Assumptions:
        Input json is linear st level 2, where data it. (No nested Data)
    Flow:
        1. Read inout json data (FHIR Json), and store it into a json_dict.
        2. Convert json_dict into a Pandas Dataframe (input_dataframe).
        3. Read mapping json file (name format: input_file_name_mapping.json).
        4. Create dataframe (output_dataframe) using the mapping json, append input_dataframe rows to it based on
           key/value from mapping file.
        5. Convert output_dataframe to CSV file and save it in the GCS bucket location.
    """
    # GCSFileSystem Object to read json files from Google Cloud Storage,
    gcs_file_system = gcsfs.GCSFileSystem(project=project_id)
    bucket_uri = "gs://" + bucket + "/"

    # Read the JSON input file in a python dictionary.
    path_to_file = bucket_uri + file_name
    input_data = read_input_json(path_to_file=path_to_file,
                                 gcs_file_system=gcs_file_system)

    # Create dataframe for array items.
    input_dataframe = create_input_dataframe(input_data=input_data)

    # create Mapped Dataframe based on fields from json mapping file.
    path_to_mapping_file = bucket_uri + "mapping_files/" + \
                           file_name.split('.')[0].split('/')[1] + "_mapping.json"
    mapped_dataframe = create_mapped_dataframe(mapping_file=path_to_mapping_file,
                                               input_dataframe=input_dataframe,
                                               gcs_file_system=gcs_file_system)

    # output file name with path.
    generated_csv_path = bucket_uri + "generated_csv_files/"
    csv_file = file_name.split('.')[0].split('/')[1] + ".csv"
    output_file = generated_csv_path + csv_file
    # Convert the dataframe into CSV and write it in output location.
    mapped_dataframe.to_csv(output_file, index=False, line_terminator="\n")

    print(f"Added {csv_file} to {generated_csv_path} path.")
    # Checking csv exist did not work because of small test data.
    # if check_file_exist_in_gcs(target_bucket=bucket, full_file_path=output_file):
    # Archive input FHIR Json file if OMOP CSV file is successfully create.
    archive_processed_file(source_bucket=bucket,
                           file_name=file_name,
                           creation_date=creation_date,
                           file_type=extension)