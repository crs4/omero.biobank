import pika
from bl.vl.utils import get_logger
import config as vlconf


def get_events_sender(logger=None):
    if vlconf.MESSAGES_QUEUE_ENGINE_ENABLED:
        return EventsSender(vlconf.MESSAGES_QUEUE_ENGINE_HOST,
                            vlconf.MESSAGES_QUEUE_ENGINE_PORT,
                            vlconf.MESSAGES_QUEUE_ENGINE_USERNAME,
                            vlconf.MESSAGES_QUEUE_ENGINE_PASSWORD,
                            vlconf.MESSAGES_QUEUE_ENGINE_QUEUE,
                            logger)
    else:
        return None


def get_events_consumer(logger=None):
    return EventsConsumer(vlconf.MESSAGES_QUEUE_ENGINE_HOST,
                          vlconf.MESSAGES_QUEUE_ENGINE_PORT,
                          vlconf.MESSAGES_QUEUE_ENGINE_USERNAME,
                          vlconf.MESSAGES_QUEUE_ENGINE_PASSWORD,
                          vlconf.MESSAGES_QUEUE_ENGINE_QUEUE,
                          logger)


class MessagesEngineConfigurationError(Exception):
    pass


class MessagesHandler(object):
    
    def __init__(self, host, port=None, user=None, password=None,
                 queue=None, logger=None):
        self.logger = logger or get_logger('messages-handler')
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.queue = queue
        self.exchange_name = 'exch_%s' % self.queue
        self.connection = None
        self.channel = None
        if self.host is None or self.queue is None:
            raise MessagesEngineConfigurationError('Check configuration values for HOST and QUEUE')

    def __get_connection_params(self):
        self.conn_params = pika.ConnectionParameters()
        if not self.host:
            raise ValueError('No host provided')
        else:
            self.conn_params.host = self.host
        if self.port:
            self.conn_params.port = self.port
        if self.user and self.password:
            credentials = pika.PlainCredentials(self.user,
                                                self.password)
            self.conn_params.credentials = credentials
        return self.conn_params

    def connect(self):
        if not self.connection:
            conn_params = self.__get_connection_params()
            self.connection = pika.BlockingConnection(conn_params)
            self.channel = self.connection.channel()
            self.__setup_network()
            self.logger.info('connection established')
        else:
            self.logger.debug('Connection object already exist')

    def disconnect(self):
        if self.connection:
            try:
                self.connection.close()
                self.channel = None
                self.connection = None
            except Exception, e:
                self.logger.error('exception raised when closing connection')
                self.logger.exception(e)

    def __setup_network(self):
        self.__declare_exchange()
        f = self.__declare_queue()
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=f.method.queue,
            routing_key='%s.graph.#' % self.queue
        )

    @property
    def is_queue_empty(self):
        self.connect()
        f = self.__declare_queue()
        if f.method.message_count == 0:
            self.logger.debug('queue is empty')
            return True
        else:
            self.logger.debug('%d messages still in queue' % f.method.message_count)
            return False

    def __declare_exchange(self):
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            type='topic',
            durable=True
        )

    def __declare_queue(self):
        frame = self.channel.queue_declare(
            queue=self.queue,
            durable=True,
            exclusive=False,
            auto_delete=False
        )
        return frame

    def __del__(self):
        self.disconnect()


class EventsSender(MessagesHandler):

    def __init__(self, host, port=None, user=None, password=None,
                 queue=None, logger=None):
        super(EventsSender, self).__init__(host, port, user, password,
                                           queue, logger)

    def send_event(self, event):
        if not self.connection:
            self.connect()

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key='%s.%s' % (self.queue, event.event_type),
            body=event.msg,
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent messages
                content_type='text/plain'
            )
        )


class EventsConsumer(MessagesHandler):

    def __init__(self, host, port=None, user=None, password=None,
                 queue=None, logger=None):
        super(EventsConsumer, self).__init__(host, port, user, password,
                                             queue, logger)

    def stop(self):
        self.logger.debug('Closing connection')
        self.channel.stop_consuming()
        self.disconnect()

    def run(self, consumer_callback):
        if not self.channel:
            self.connect()
        try:
            self.channel.basic_consume(consumer_callback, queue=self.queue)
            self.channel.start_consuming()
            self.logger.info('Start consuming messages')
        except KeyboardInterrupt:
            self.logger.info('Captured keyboard interrupt, stopping')
        except Exception, e:
            self.logger.critical('Exception captured: %s' % e)
            raise e
        finally:
            try:
                self.stop()
            except:
                self.logger.error('Error occurred while calling stop() method')
                pass