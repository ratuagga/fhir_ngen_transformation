import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run(input_file, project_id, job_name, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  output_path = "gs://beam_test_bucket-1/file_output/" + job_name + ".txt"
  temp_location = "gs://beam_test_bucket-1/temp_files"
  staging_location = "gs://beam_test_bucket-1/staging_dir"
  pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    region='us-central1',
    setup_file='./setup.py',
    project=project_id,
    job_name=job_name,
    temp_location=temp_location,
    staging_location=staging_location
  )
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as pipeline:

    # Read the text file[pattern] into a PCollection.
    lines = pipeline | 'Read' >> ReadFromText(input_file)

    counts = (
        lines
        | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
        | 'lowercase' >> beam.Map(str.lower)
        | 'PairWIthOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum))

    # Format the counts into a PCollection of strings.
    def format_result(word, count):
      return '%s: %d' % (word, count)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(output_path)
    # pipeline.run() gets called automatically.