"""Converting FHIR JSON file to OMOP CSV file."""
import gcsfs
import json
import pandas
import datetime
import pytz
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


def data_convert(element, data_type):
    """Convert element into the given data_type."""
    try:
        if isinstance(element, dict) or isinstance(element, list):
            pass
        elif data_type == 'string' and not isinstance(element, str):
            element = str(element)
        elif data_type == "integer" and not isinstance(element, int):
            element = int(element)
        elif data_type == "float" and not isinstance(element, float):
            element = float(element)
        elif data_type == "date" and isinstance(element, str):
            element = element.split("T")[0]
        elif data_type == "timestamp" and isinstance(element, str):
            # Input sample: "2009-07-04T17:20:00.515-04:00"
            inp_ts_format = "%Y-%m-%dT%H:%M:%S.%f%z"
            date = datetime.datetime.strptime(element, inp_ts_format)
            element = str(date.astimezone(pytz.UTC)).replace("+00:00", " UTC")
    except:
        raise Exception(f"Error during converting {element} in {data_type}.")

    return element


def create_mapped_dataframe(mapping_file: str,
                            input_data,
                            gcs_file_system) -> pandas.DataFrame:
    """Map the input_data to create a mapped output_dataframe."""
    try:
        with gcs_file_system.open(mapping_file) as file:
            mapping_dict = json.load(file)
    except:
        raise Exception(f"Reading {mapping_file} file encountered an error.")

    # Create Dataframe with mapping_dict keys as columns.
    csv_cols = mapping_dict.keys()
    output_dataframe = pandas.DataFrame(columns=csv_cols)

    # Convert the input data into list type.
    if not isinstance(input_data, list):
        input_data = [input_data]

    # Create a Record and append it in the output_dataframe
    for index, row in enumerate(input_data):
        record = []
        for key, value in mapping_dict.items():
            if value:
                # value = ['field', 'datatype'] format
                depth = value[0].split('.')
                val = row
                for field in depth:
                    # if val is a list, pick 1st element of the list.
                    if isinstance(val, list):
                        val = val[0]
                    val = val.get(field)
                    val = data_convert(val, value[1])
                record.append(val)
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
    Returns:
        None; Newly created CSV data file stored in 'generated_csv' directory of the bucket.

    Assumptions:
        Input json is linear st level 2, where data it. (No nested Data)
    Flow:
        1. Read inout json data (FHIR Json), and store it into a json_dict.
        2. Read mapping json file (name format: input_file_name_mapping.json).
        3. Create dataframe (output_dataframe) using the mapping json, append input_data rows to it based on key/value from mapping file.
        4. Convert output_dataframe to CSV file and save it in the GCS bucket location.
    """
    # GCSFileSystem Object to read json files from Google Cloud Storage,
    gcs_file_system = gcsfs.GCSFileSystem(project=project_id)
    bucket_uri = "gs://" + bucket + "/"

    # Read the JSON input file in a python dictionary.
    path_to_file = bucket_uri + file_name
    input_data = read_input_json(path_to_file=path_to_file,
    					         gcs_file_system=gcs_file_system)

    # create Mapped Dataframe based on fields from json mapping file.
    path_to_mapping_file = bucket_uri + "mapping_files/" + \
        file_name.split('.')[0].split('/')[1] + "_mapping.json"
    mapped_dataframe = create_mapped_dataframe(mapping_file=path_to_mapping_file,
                                               input_data=input_data,
                                               gcs_file_system=gcs_file_system)

    # output file name with path.
    generated_csv_path = bucket_uri + "generated_csv_files/"
    csv_file = file_name.split('.')[0].split('/')[1] + ".csv"
    output_file = generated_csv_path + csv_file
    # Convert the dataframe into CSV and write it in output location.
    mapped_dataframe.to_csv(output_file, index=False, index_label=False, line_terminator="\n")

    print(f"Added {csv_file} to {generated_csv_path} path.")
    # Checking csv exist did not work because of small test data.
    # if check_file_exist_in_gcs(target_bucket=bucket, full_file_path=output_file):
    # Archive input FHIR Json file if OMOP CSV file is successfully create.
    archive_processed_file(source_bucket=bucket,
                           file_name=file_name,
                           creation_date=creation_date,
                           file_type=extension)
