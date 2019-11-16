import datetime as dt
import pytz
from apache_beam.io.gcp.internal.clients import bigquery
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import os


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

    @staticmethod
    def gen_params_dates(num_weeks_back, num_weeks_skip, to_today=False):
        TZ = 'Asia/Jerusalem'
        result = list()
        for weeks_back in range(num_weeks_skip, num_weeks_back):

            today = dt.datetime.now(pytz.timezone(TZ))
            yesterday = today - dt.timedelta(1)

            # date_start : Make it a sunday
            date_start = yesterday - dt.timedelta((yesterday.weekday() + 1) % 7)
            date_start = date_start - dt.timedelta(weeks_back * 7)

            # If num_weeks_skip == 0
            if weeks_back == 0:

                # If to_today=True & today is not a sunday
                if to_today and (today.weekday() != 6):
                    date_end = today
                else:
                    date_end = yesterday

            # num_weeks_skip != 0
            else:
                date_end = date_start + dt.timedelta(6)

            record = {'date_from': date_start.strftime('%Y-%m-%d'), 'date_to': date_end.strftime('%Y-%m-%d')}
            result.append(record)

            if to_today and weeks_back == 0 and today.weekday() == 6:
                record = {'date_from': today.strftime('%Y-%m-%d'), 'date_to': today.strftime('%Y-%m-%d')}
                result.append(record)
        return result


if __name__ == "__main__":

    gcp_project_id = 'my-project-d'
    gcp_gs_temp_location = 'google-storage-temp-location'
    gcp_gs_staging_location = 'google-storage-staging-location'
    bq_dataset_id = 'my-dataset-id'

    # Creating Sharded tables
    bq_table_id = 'df_data_'
    gcp_job_name = 'df-description-of-job-to-run'

    datauploader = DataExtractor(gcp_project_id, bq_dataset_id)

    aggregated_data = "data-extracted-from-api-list-of-dictionary"
    pre_table_schema = [
        ('campaign', 'STRING', 'NULLABLE'),
        ('clicks', 'INTEGER', 'NULLABLE')
    ]

    datauploader.run(
        gcp_job_name,
        gcp_gs_temp_location,
        gcp_gs_staging_location,
        aggregated_data,
        bq_table_id,
        pre_table_schema
    )
