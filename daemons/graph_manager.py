from bl.vl.kb.messages import EventsConsumer
from bl.vl.utils import get_logger, decode_dict
from bl.vl.kb.drivers.graph import build_driver
import sys
import json


class GraphManagerDaemon(object):

    def __init__(self, logger=None):
        self.actions_mapping = {
            'NODE_CREATE': self.create_node,
            'EDGE_CREATE': self.create_edge,
            'NODE_DELETE': self.delete_node,
            'EDGE_DELETE': self.delete_edge,
            'EDGE_UPDATE': self.update_edge,
        }

        if not logger:
            logger = get_logger('graph_manager_daemon')
        self.messages_consumer = EventsConsumer(logger)
        self.graph_driver = build_driver()

    def consume_message(self, channel, method, head, body):
        msg = decode_dict(json.loads(body))
        try:
            self.actions_mapping[msg['action']](msg)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except:
            channel.basic_nack(delivery_tag=method.delivery_tag)

    def start_consume(self):
        self.messages_consumer.connect()
        self.messages_consumer.run(self.consume_message)

    def create_node(self, msg):
        nid = self.graph_driver.save_node(msg['details'])

    def create_edge(self, msg):
        eid = self.graph_driver.save_edge(msg['details'], msg['source_node'],
                                          msg['dest_node'])

    def delete_node(self, msg):
        self.graph_driver.delete_node(msg['target'])

    def delete_edge(self, msg):
        self.graph_driver.delete_edge(msg['target'])

    def update_edge(self, msg):
        print 'Update edge!'


def main(argv):
    daemon = GraphManagerDaemon()
    daemon.start_consume()

if __name__ == '__main__':
    main(sys.argv)