# FHIR NGEN TRANSFORMATION
> Process to take data from a FHIR format json file to BigQuery Table.

### Flow walk through:
1. Any new FHIR-Formatted JSON file (Input File) when added into the GCP Bucket, will trigger a Cloud Function (Entry point = 'fhir_to_omop_trigger()')
2. First task is to map the 'Input File' to corresponding OMOP-Format ('OMOP Mapping'). 
   1. This piece is currently under development while this document is being written.
   2. For testing, a dummy 'Input File' was created from Test_Data in All-Of-Us/Curation repository.
   3. The 'Input File' then gets mapped to OMOP-format based on corresponding mapping file.
   4. The OMOP-Formatted data then gets saved as CSV file in the same bucket where 'Input File' was picked up from.
   5. Archive 'Input File' (Moving the 'Input File' in a new GCP Bucket).
3. The newly created OMOP-Formatted CSV file (Output File) then triggers the same Cloud Function again.
4. Data from the 'Output File' than gets ***appended*** into an ***existing table*** in BigQuery Dataset.
5. Archive 'Output File' (Moving the 'Output File' in the same Bucket where 'Input File' was moved).


### Sample File Locations and BigQuery Table Name:
1. **Input File:** gs://*GCP_bucket*/*Input_file_folder*/*Input_file.json*
2. **Output File:** gs://*GCP_bucket*/generated_csv_files/
3. **Mapping File:** gs://*GCP_bucket*/mapping_files/*Input_file_name_without_extension*_mapping.json
4. **Archive Bucket:** gs://test_bucket_for_cloud_trigger_archive_files/
   1. If this bucket name is changed, update ***utils.py/archive_processed_file()/destination_bucket***
5. **BigQuery Table:** project_id.test_csv_load_dataset.*Input_File_name_without_extension*
   1. Table Schema ***MUST*** match the OMOP-Mapping of the corresponding Output File.


### Supporting Files:
1. Test Input Json
2. Mapping File for FHIR-to-OMOP. 
