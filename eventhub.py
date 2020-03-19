from __future__ import division, print_function
from apache_beam import PTransform, ParDo, DoFn, Create
from azure.eventhub import EventHubProducerClient, EventData

class EHProduce(PTransform):

    def __init__(self, eventhub_name=None, connection_str=None):
        super(EHProduce, self).__init__()
        self._attributes = dict(
            eventhub_name=eventhub_name,
            connection_str=connection_str)

    def expand(self, pcoll):
        return (
            pcoll
            | ParDo(_ProduceEHMessage(self._attributes))
        )

class _ProduceEHMessage(DoFn):
    def __init__(self, attributes, *args, **kwargs):
        super(_ProduceEHMessage, self).__init__(*args, **kwargs)
        self.attributes = attributes

    def start_bundle(self):
        self.client = EventHubProducerClient.from_connection_string(self.attributes["connection_str"], eventhub_name=self.attributes["eventhub_name"])
        self.event_data_batch = self.client.create_batch()

    def finish_bundle(self):
        if (self.event_data_batch._count) > 0:
            self.client.send_batch(self.event_data_batch)
            self.event_data_batch = self.client.create_batch()
        self.client.close()

    def process(self, element):
        try:
            self.event_data_batch.add(EventData(element.encode()))
        except ValueError:
            self.client.send_batch(self.event_data_batch)
            self.event_data_batch = self.client.create_batch()
            self.event_data_batch.add(EventData(element.encode()))
