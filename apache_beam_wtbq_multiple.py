import datetime as dt
import pytz
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import os

print("Loading Apache Beam Dataflow Wrapper")
CREDENTIALS = r'Path-to-credential-file-json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS
# del os.environ['GOOGLE_APPLICATION_CREDENTIALS']

class DataExtractor(object):

    def __init__(self, gcp_project_id, bq_dataset_id):
        self.gcp_project_id = gcp_project_id
        self.bq_dataset_id = bq_dataset_id

    def run(self, gcp_job_name, gcp_gs_staging_location, gcp_gs_temp_location,
            data_to_upload, bq_table_id, schema):
        pipeline_options = PipelineOptions()
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.gcp_project_id
        google_cloud_options.job_name = gcp_job_name
        google_cloud_options.staging_location = gcp_gs_staging_location
        google_cloud_options.temp_location = gcp_gs_temp_location
        pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

        # Defining a Table Schema for Big Query Upload
        bq_table_schema = bigquery.TableSchema()
        for field_name, field_type, field_mode in schema:
            schema_field = bigquery.TableFieldSchema()
            schema_field.name = field_name
            schema_field.type = field_type
            schema_field.mode = field_mode
            bq_table_schema.fields.append(schema_field)

        with beam.Pipeline(options=pipeline_options) as p:
            for ads_dd in data_to_upload:
                table_spec = bigquery.TableReference(
                    projectId=self.gcp_project_id,
                    datasetId=self.bq_dataset_id,
                    tableId=bq_table_id + ads_dd['data_period'])
                bq_uploader = (
                        p
                        | 'reading_data_{}'.format(ads_dd['data_period']) >> beam.Create(ads_dd['extracted_data'])
                        | 'writing_data_{}'.format(ads_dd['data_period']) >> beam.io.WriteToBigQuery(
                    table_spec,
                    schema=bq_table_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                )
                )

    def run_multiple(self, gcp_job_name, gcp_gs_staging_location, gcp_gs_temp_location, ads_tables_upload):
        pipeline_options = PipelineOptions()
        google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.gcp_project_id
        google_cloud_options.job_name = gcp_job_name
        google_cloud_options.staging_location = gcp_gs_staging_location
        google_cloud_options.temp_location = gcp_gs_temp_location
        pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'

        with beam.Pipeline(options=pipeline_options) as p:
            for tables_dd in ads_tables_upload:
                # Defining a Table Schema for Big Query Upload
                bq_table_schema = bigquery.TableSchema()
                for field_name, field_type, field_mode in tables_dd.get('pre_table_schema'):
                    schema_field = bigquery.TableFieldSchema()
                    schema_field.name = field_name
                    schema_field.type = field_type
                    schema_field.mode = field_mode
                    bq_table_schema.fields.append(schema_field)

                for ads_dd in tables_dd.get('data'):
                    table_spec = bigquery.TableReference(
                        projectId=self.gcp_project_id,
                        datasetId=self.bq_dataset_id,
                        tableId=tables_dd.get('bq_table_id') + ads_dd['data_period'])
                    bq_uploader = (
                            p
                            | 'read_{}_{}'.format(tables_dd.get('bq_table_id'), ads_dd['data_period']) >> beam.Create(ads_dd['extracted_data'])
                            | 'write_{}_{}'.format(tables_dd.get('bq_table_id'), ads_dd['data_period']) >> beam.io.WriteToBigQuery(
                                                        table_spec,
                                                        schema=bq_table_schema,
                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                                                    )
                                )
