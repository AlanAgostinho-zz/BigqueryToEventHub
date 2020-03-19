from __future__ import absolute_import
from __future__ import print_function
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from datetime import datetime
from kafkaio import KafkaProduce

import simplejson as json
import argparse
import logging
import apache_beam as beam

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bql',
                        dest='bql',
                        help='bigquery sql to extract req columns and rows.')
    parser.add_argument('--output',
                        dest='output',
                        help='gcs output location for parquet files.')
    parser.add_argument('--connection_string',
                        dest='connection_string',
                        help='cconnection_string for kafka.')
    parser.add_argument('--servers',
                        dest='servers',
                        help='servers connection for kafka.')
    parser.add_argument('--topic',
                        dest='topic',
                        help='topic connection for kafka.')
    parser.add_argument('--bucket',
                        dest='bucket_name',
                        help='bucket connection for kafka.')
    known_args, \
    pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)
    bq_table_source = known_args.bql.split('`')[1].split('.')

    #irá gerar o arquivo dentro do bucket database/tabela/templates/data.txt
    name = bq_table_source[1] + '/' + bq_table_source[2] + '/templates/data.txt'
    storage_client = storage.Client()
    bucket = storage_client.bucket(known_args.bucket_name)
    stats = storage.Blob(bucket=bucket, name=name).exists(storage_client)

    if (stats == True):
        #read data from arquivo
        bucket = storage_client.get_bucket(known_args.bucket_name)
        blob = bucket.get_blob(name)
        downloaded_blob = blob.download_as_string()
        downloaded_blob = downloaded_blob.decode('utf-8')
    else:
        bucket = storage_client.get_bucket(known_args.bucket_name)
        blob = bucket.blob(name)
        blob.upload_from_string('1900-01-01 00:00:00')
        downloaded_blob = "1900-01-01 00:00:00"

    # update data from arquivo
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    bucket = storage_client.get_bucket(known_args.bucket_name)
    blob = bucket.blob(name)
    known_args.bql = known_args.bql.replace("replace_date_inicial", "'" + downloaded_blob + "'")
    known_args.bql = known_args.bql.replace("replace_date_final", "'" + timestamp + "'")

    p = beam.Pipeline(options=options)

    p | 'Input: QueryTable' >> (beam.io.Read(beam.io.BigQuerySource(query=known_args.bql, use_standard_sql=True))) \
      | 'FormatOutput' >> beam.Map(json.dumps) \
      | "Pushing messages to Kafka" >> KafkaProduce(
            topic=known_args.topic,
            servers=known_args.servers,
            connection_string=known_args.connection_string
        )

    result = p.run()
    result.wait_until_finish()  # Makes job to display all the logs

    blob.upload_from_string(timestamp)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()