from __future__ import absolute_import, division, print_function
from __future__ import print_function
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from eventhub import EHProduce

import simplejson as json
import argparse
import logging
import apache_beam as beam


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--query', type=str)
        parser.add_argument('--connection_string',
                            dest='connection_str',
                            help='connection_str for eventhub.')
        parser.add_argument('--servers',
                            dest='servers',
                            help='servers connection for eventhub.')
        parser.add_argument('--eventhub_name',
                            dest='eventhub_name',
                            help='topic connection for eventhub.')

def run(argv=None):
    pipeline_options = PipelineOptions()
    p = beam.Pipeline(options=pipeline_options)

    user_options = pipeline_options.view_as(UserOptions)

    p | 'Input: QueryTable' >> beam.io.Read(beam.io.BigQuerySource(query=user_options.query.get(), use_standard_sql=True)) \
      | 'FormatOutput' >> beam.Map(json.dumps) \
      | "Pushing messages to eventHub" >> EHProduce(
                                                        eventhub_name=user_options.eventhub_name,
                                                        connection_str=user_options.connection_str
                                                      )
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()