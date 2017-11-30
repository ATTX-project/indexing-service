# import threading
from amqpstorm import Message
from amqpstorm import Connection
from esindex.utils.logs import app_logger


class Publisher(object):
    """Provenance message pubblisher."""

    def __init__(self, hostname='127.0.0.1',
                 username='guest', password='guest',
                 queue='base.queue'):
        """Consumer init function."""
        self.hostname = hostname
        self.username = username
        self.password = password
        self.queue = queue

    # def push(self, message):
    #     """Push message."""
    #     thread = threading.Thread(target=self.start(message))
    #     thread.daemon = True
    #     thread.start()

    def push(self, message):
        """Start channel to provenance inbox."""
        with Connection(self.hostname, self.username, self.password) as connection:
            with connection.channel() as channel:
                channel.queue.declare(self.queue)
                properties = {
                    'content_type': 'application/json'
                }
                message = Message.create(channel, message, properties)
                message.publish(self.queue)
                app_logger.info('Pushed message: {0}.'.format(message))
