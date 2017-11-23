import time
import json
import threading
import amqpstorm
from amqpstorm import Message
from amqpstorm import Connection
from esindex.utils.logs import app_logger
from esindex.applib.construct_message import replace_message, add_message
from esindex.applib.construct_message import response_message


class ScalableRpcServer(object):
    """Graph Manger RPC server."""

    def __init__(self, hostname='127.0.0.1',
                 username='guest', password='guest',
                 rpc_queue='base.rpc_queue',
                 number_of_consumers=5, max_retries=None):
        """Server init function."""
        self.hostname = hostname
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.number_of_consumers = number_of_consumers
        self.max_retries = max_retries
        self._connection = None
        self._consumers = []
        self._stopped = threading.Event()

    def start_server(self):
        """Start the RPC Server.

        :return:
        """
        self._stopped.clear()
        if not self._connection or self._connection.is_closed:
            self._create_connection()
        while not self._stopped.is_set():
            try:
                # Check our connection for errors.
                self._connection.check_for_errors()
                self._update_consumers()
            except amqpstorm.AMQPError as why:
                # If an error occurs, re-connect and let update_consumers
                # re-open the channels.
                app_logger.error(why)
                self._stop_consumers()
                self._create_connection()
            time.sleep(1)

    def increase_consumers(self):
        """Add one more consumer.

        :return:
        """
        if self.number_of_consumers <= 20:
            self.number_of_consumers += 1

    def decrease_consumers(self):
        """Stop one consumer.

        :return:
        """
        if self.number_of_consumers > 0:
            self.number_of_consumers -= 1

    def stop(self):
        """Stop all consumers.

        :return:
        """
        while self._consumers:
            consumer = self._consumers.pop()
            consumer.stop()
        self._stopped.set()
        self._connection.close()

    def _create_connection(self):
        """Create a connection.

        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            if self._stopped.is_set():
                break
            try:
                self._connection = Connection(self.hostname,
                                              self.username,
                                              self.password)
                app_logger.info('Established connection with AMQP server {0}'.format(self._connection))
                break
            except amqpstorm.AMQPError as why:
                app_logger.error(why)
                if self.max_retries and attempts > self.max_retries:
                    raise Exception('max number of retries reached')
                time.sleep(min(attempts * 2, 30))
            except KeyboardInterrupt:
                break

    def _update_consumers(self):
        """Update Consumers.

            - Add more if requested.
            - Make sure the consumers are healthy.
            - Remove excess consumers.

        :return:
        """
        # Do we need to start more consumers.
        consumer_to_start = \
            min(max(self.number_of_consumers - len(self._consumers), 0), 2)
        for _ in range(consumer_to_start):
            consumer = Consumer(self.rpc_queue)
            self._start_consumer(consumer)
            self._consumers.append(consumer)

        # Check that all our consumers are active.
        for consumer in self._consumers:
            if consumer.active:
                continue
            self._start_consumer(consumer)
            break

        # Do we have any overflow of consumers.
        self._stop_consumers(self.number_of_consumers)

    def _stop_consumers(self, number_of_consumers=0):
        """Stop a specific number of consumers.

        :param number_of_consumers:
        :return:
        """
        while len(self._consumers) > number_of_consumers:
            consumer = self._consumers.pop()
            consumer.stop()

    def _start_consumer(self, consumer):
        """Start a consumer as a new Thread.

        :param Consumer consumer:
        :return:
        """
        thread = threading.Thread(target=consumer.start,
                                  args=(self._connection,))
        thread.daemon = True
        thread.start()


class Consumer(object):
    """Handle requests in a consumer."""

    def __init__(self, rpc_queue):
        """Consumer init function."""
        self.rpc_queue = rpc_queue
        self.channel = None
        self.active = False

    def start(self, connection):
        """Start the Consumers."""
        self.channel = None
        try:
            self.active = True
            self.channel = connection.channel(rpc_timeout=10)
            self.channel.basic.qos(1)
            self.channel.queue.declare(self.rpc_queue)
            self.channel.basic.consume(self, self.rpc_queue, no_ack=False)
            self.channel.start_consuming(to_tuple=False)
            app_logger.info('Connected to queue {0}'.format(self.queue))
            if not self.channel.consumer_tags:
                # Only close the channel if there is nothing consuming.
                # This is to allow messages that are still being processed
                # in __call__ to finish processing.
                self.channel.close()
        except amqpstorm.AMQPError:
            pass
        finally:
            self.active = False

    def stop(self):
        """Stop the Consumers."""
        if self.channel:
            self.channel.close()

    def _handle_message(self, message):
        """Handle ES index messages."""
        message_data = json.loads(message.body)
        action = message_data["payload"]["indexingServiceInput"]["task"]
        if action == "replace":
            return str(replace_message(message_data))
        elif action == "add":
            return str(add_message(message_data))
        else:
            raise KeyError("Missing action or activity not specified.")

    def __call__(self, message):
        """Process the RPC Payload.

        :param Message message:
        :return:
        """
        try:
            processed_message = self._handle_message(message)
        except Exception as e:
            app_logger.error('Something went wrong: {0}'.format(e))
            properties = {
                'correlation_id': message.correlation_id
            }
            error_message = "Error Type: {0}, with message: {1}".format(e.__class__.__name__, e.message)
            message_data = json.loads(message.body)
            processed_message = response_message(message_data["provenance"], status="error", status_messsage=error_message)
            response = Message.create(message.channel, str(json.dumps(processed_message, indent=4, separators=(',', ': '))), properties)
            response.publish(message.reply_to)
            message.reject(requeue=False)
        else:
            properties = {
                'correlation_id': message.correlation_id
            }
            response = Message.create(message.channel, processed_message, properties)
            response.publish(message.reply_to)
            message.ack()
