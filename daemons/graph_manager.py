from bl.vl.kb.messages import get_events_consumer, MessagesEngineAuthenticationError, \
    MessageEngineConnectionError
from bl.vl.graph import build_driver
from bl.vl.graph.errors import MissingEdgeError, GraphAuthenticationError, \
    GraphConnectionError
from bl.vl.kb.events import decode_event, InvalidMessageError
import sys
import logging
import argparse
import os
from logging.handlers import RotatingFileHandler
from bl.vl.utils import LOG_FORMAT, LOG_DATEFMT, get_logger


class GraphManagerDaemon(object):

    LOGGER_LABEL = 'graph_manager_daemon'
    LOG_MAX_SIZE = 100*1024*1024
    LOG_BACKUP_COUNT = 3

    def __init__(self, log_file, log_level, pid_file):
        self.actions_mapping = {
            'NODE_CREATE': self.create_node,
            'EDGE_CREATE': self.create_edge,
            'NODE_DELETE': self.delete_node,
            'EDGE_DELETE': self.delete_edge,
            'EDGES_DELETE': self.delete_edges,
            'EDGE_UPDATE': self.update_edge,
            'COLLECTION_ITEM_CREATE': self.create_collection_item,
        }

        self.logger = get_logger('graph_manager_daemon', log_level, log_file)
        self.pid_file = pid_file

        try:
            self.messages_consumer = get_events_consumer(self.logger, self.consume_message,
                                                         self.destroy_pid)
            self.graph_driver = build_driver()
        except GraphAuthenticationError, gr_auth_error:
            self.logger.critical(gr_auth_error.message)
            sys.exit(gr_auth_error.message)

    def check_pid_file(self):
        if os.path.isfile(self.pid_file):
            self.logger.info('Another Graph manager is running, exit')
            sys.exit(0)

    def create_pid(self):
        pid = str(os.getpid())
        with open(self.pid_file, 'w') as ofile:
            ofile.write(pid)

    def destroy_pid(self):
        os.remove(self.pid_file)

    def __get_logger(self, filename, log_level):
        logger = logging.getLogger(self.LOGGER_LABEL)
        logger.setLevel(getattr(logging, log_level))
        handler = RotatingFileHandler(filename, maxBytes=self.LOG_MAX_SIZE,
                                      backupCount=self.LOG_BACKUP_COUNT)
        handler.setLevel(getattr(logging, log_level))
        formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATEFMT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def consume_message(self, channel, method, properties, body):
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
        except GraphConnectionError, gre:
            self.logger.error(gre.message)
            channel.basic_nack(delivery_tag=method.delivery_tag)
        except Exception, e:
            self.logger.exception(e)
            channel.basic_nack(delivery_tag=method.delivery_tag)

    def start_consume(self):
        self.create_pid()
        self.logger.info('Connecting to messages queue')
        try:
            self.messages_consumer.connect()
        except MessageEngineConnectionError, me_conn_error:
            self.logger.critical(me_conn_error.message)
            sys.exit(me_conn_error.message)
        except MessagesEngineAuthenticationError, me_auth_error:
            self.logger.critical(me_auth_error.message)
            sys.exit(me_auth_error.message)
        self.logger.info('Start consuming messages')
        self.messages_consumer.run()

    def create_node(self, msg):
        nid = self.graph_driver.save_node(msg['details'])
        self.logger.info('Saved new node, assigned ID is %d' % nid)

    def create_edge(self, msg):
        eid = self.graph_driver.save_edge(msg['details'], msg['source_node'],
                                          msg['dest_node'])
        self.logger.info('Saved new edge, assigned ID is %d' % eid)

    def create_collection_item(self, msg):
        eid = self.graph_driver.save_collection_item(msg['item_node'],
                                                     msg['collection_node'])
        self.logger.info('Saved new collection item edge, assigned ID is %d' % eid)

    def delete_node(self, msg):
        self.graph_driver.delete_node(msg['target'])
        self.logger.info('Node successfully deleted')

    def delete_edge(self, msg):
        self.graph_driver.delete_edge(msg['target'])
        self.logger.info('Edge successfully deleted')

    def delete_edges(self, msg):
        self.graph_driver.delete_edges(msg['target'])
        self.logger.info('Edges successfully deleted')

    def update_edge(self, msg):
        raise NotImplementedError()


def make_parser():
    parser = argparse.ArgumentParser(description='Run the daemon that fills the graph database')
    parser.add_argument('--logfile', type=str, help='log file name')
    parser.add_argument('--loglevel', type=str, help='log level',
                        default='INFO')
    parser.add_argument('--pid-file', type=str, help='PID file',
                        default='graph_manager.pid')
    return parser


def main(argv):
    parser = make_parser()
    args = parser.parse_args(argv)
    daemon = GraphManagerDaemon(args.logfile, args.loglevel, args.pid_file)
    daemon.check_pid_file()
    daemon.start_consume()

if __name__ == '__main__':
    main(sys.argv[1:])
