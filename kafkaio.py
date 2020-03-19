from __future__ import division, print_function

from apache_beam import PTransform, ParDo, DoFn, Create
from kafka import KafkaConsumer, KafkaProducer

"""A :class:`~apache_beam.transforms.ptransform.PTransform` for pushing messages
into an Apache Kafka topic. This class expects a tuple with the first element being the message key
and the second element being the message. The transform uses `KafkaProducer`
from the `kafka` python library.

Args:
    topic: Kafka topic to publish to
    servers: list of Kafka servers to listen to

Examples:
    Examples:
    Pushing message to a Kafka Topic `notifications` ::

        from __future__ import print_function
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        from beam_nuggets.io import kafkaio

        with beam.Pipeline(options=PipelineOptions()) as p:
            notifications = (p
                             | "Creating data" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
                             | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
                                                                                    topic='notifications',
                                                                                    servers="localhost:9092"
                                                                                )
                            )
            notifications | 'Writing to stdout' >> beam.Map(print)

    The output will be something like ::

        ("dev_1", '{"device": "0001", status": "healthy"}')

    Where the key is the Kafka topic published to and the element is the Kafka message produced
"""

class KafkaProduce(PTransform):

    def __init__(self, topic=None, servers='127.0.0.1:9092', connection_string=None):
        """Initializes ``KafkaProduce``
        """
        super(KafkaProduce, self).__init__()
        self._attributes = dict(
            topic=topic,
            servers=servers,
            connection_string=connection_string)

    def expand(self, pcoll):
        return (
            pcoll
            | ParDo(_ProduceKafkaMessage(self._attributes))
        )

class _ProduceKafkaMessage(DoFn):
    def __init__(self, attributes, *args, **kwargs):
        super(_ProduceKafkaMessage, self).__init__(*args, **kwargs)
        self.attributes = attributes

    def start_bundle(self):
        self._producer = KafkaProducer(
                                            bootstrap_servers=self.attributes["servers"],
                                            security_protocol='SASL_SSL',  # this is likely the problem
                                            sasl_mechanism='PLAIN',
                                            sasl_plain_username='$ConnectionString',
                                            sasl_plain_password=self.attributes["connection_string"],
                                            api_version=(2, 0, 0),
                                            send_buffer_bytes=20000,
                                            buffer_memory=20000,
                                            acks='all',
                                            max_request_size=20000,
                                            max_block_ms=20000,
                                            retries=2
                                    )

    def finish_bundle(self):
        self._producer.close()

    def process(self, element):
        try:
            self._producer.send(self.attributes['topic'], element.encode(), None)
            yield element
        except Exception as e:
            raise
