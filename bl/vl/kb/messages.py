import pika

import config as vlconf


class MessagesHandler(object):
    
    def __init__(self, kb):
        self.logger = kb.logger
        self.host = vlconf.QUEUE_MANAGER_ENGINE_HOST
        self.port = vlconf.QUEUE_MANAGER_ENGINE_PORT
        self.user = vlconf.QUEUE_MANAGER_ENGINE_USERNAME
        self.passwd = vlconf.QUEUE_MANAGER_ENGINE_PASSWORD
        self.queue = vlconf.QUEUE_MANAGER_ENGINE_QUEUE
        self.exchange_name = 'exch_%s' % self.queue
        self.connection = None
        self.channel = None

    def __get_connection_params(self):
        self.conn_params = pika.ConnectionParameters()
        if not self.host:
            raise ValueError('No queue manager host provided')
        else:
            self.conn_params.host = self.host
        if self.port:
            self.conn_params.port = self.port
        if self.user and self.passwd:
            credentials = pika.PlainCredentials(self.user,
                                                self.passwd)
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
            routing_key=self.queue
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
            type='direct',
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

    def __init__(self, kb):
        super(MessagesHandler, self).__init__(kb)


class EventsConsumer(MessagesHandler):

    def __init__(self, kb):
        super(MessagesHandler, self).__init__(kb)