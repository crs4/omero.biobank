from bl.vl.kb.messages import EventsConsumer
from bl.vl.graph import build_driver
from bl.vl.graph.neo4j import MissingEdgeError
from bl.vl.kb.events import decode_event, InvalidMessageError
import sys
import logging
import argparse
from logging.handlers import RotatingFileHandler


class GraphManagerDaemon(object):

    LOGGER_LABEL = 'graph_manager_daemon'
    LOG_FORMAT = '%(asctime)s|%(levelname)-8s|%(message)s'
    LOG_DATEFMT = '%Y-%m-%d %H:%M:%S'
    LOG_MAX_SIZE = 100*1024*1024
    LOG_BACKUP_COUNT = 3

    def __init__(self, log_file=None, log_level='INFO'):
        self.actions_mapping = {
            'NODE_CREATE': self.create_node,
            'EDGE_CREATE': self.create_edge,
            'NODE_DELETE': self.delete_node,
            'EDGE_DELETE': self.delete_edge,
            'EDGE_UPDATE': self.update_edge,
        }

        if not log_file:
            self.logger = logging.getLogger(self.LOGGER_LABEL)
            self.logger.setLevel(getattr(logging, log_level))
        else:
            self.logger = self.__get_logger(log_file, log_level)
        self.messages_consumer = EventsConsumer(self.logger)
        self.graph_driver = build_driver()

    def __get_logger(self, filename, log_level):
        logger = logging.getLogger(self.LOGGER_LABEL)
        logger.setLevel(getattr(logging, log_level))
        handler = RotatingFileHandler(filename, maxBytes=self.LOG_MAX_SIZE,
                                      backupCount=self.LOG_BACKUP_COUNT)
        handler.setLevel(getattr(logging, log_level))
        formatter = logging.Formatter(self.LOG_FORMAT, datefmt=self.LOG_DATEFMT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def consume_message(self, channel, method, head, body):
        routing_key = method.routing_key
        self.logger.debug('Validating message')
        try:
            event = decode_event(routing_key, body)
        except InvalidMessageError:
            self.logger.error('Invalid message %r, removing from queue' % body)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return
        self.logger.debug('Processing message %r' % event.data)
        try:
            self.actions_mapping[event.data['action']](event.data)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except MissingEdgeError:
            # In Neo4J, if one of the nodes connected by an edge is deleted, the edge is automatically
            # deleted as well. Log the event as warning, send an ack and continue
            self.logger.warning('Unable to find edge for message %r, sending ack for message' % event.data)
            channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception, e:
            self.logger.exception(e)
            channel.basic_nack(delivery_tag=method.delivery_tag)

    def start_consume(self):
        self.logger.info('Connecting to messages queue')
        self.messages_consumer.connect()
        self.logger.info('Start consuming messages')
        self.messages_consumer.run(self.consume_message)

    def create_node(self, msg):
        nid = self.graph_driver.save_node(msg['details'])
        self.logger.info('Saved new node, assigned ID is %d' % nid)

    def create_edge(self, msg):
        eid = self.graph_driver.save_edge(msg['details'], msg['source_node'],
                                          msg['dest_node'])
        self.logger.info('Saved new edge, assigned ID is %d' % eid)

    def delete_node(self, msg):
        self.graph_driver.destroy_node(msg['target'])
        self.logger.info('Node successfully deleted')

    def delete_edge(self, msg):
        self.graph_driver.destroy_edge(msg['target'])
        self.logger.info('Edge successfully deleted')

    def update_edge(self, msg):
        raise NotImplementedError()


def make_parser():
    parser = argparse.ArgumentParser(description='Run the daemon that fills the graph database')
    parser.add_argument('--logfile', type=str, help='log file name')
    parser.add_argument('--loglevel', type=str, help='log level',
                        default='INFO')
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    daemon = GraphManagerDaemon(args.logfile, args.loglevel)
    daemon.start_consume()

if __name__ == '__main__':
    main(sys.argv[1:])