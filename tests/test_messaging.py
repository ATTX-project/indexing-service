import unittest
# from rdflib import Graph
from mock import patch
from esindex.applib.messaging import ScalableRpcServer, Consumer
from esindex.applib.messaging_publish import Publisher
# from amqpstorm import Connection
# from amqpstorm.tests.utility import FakeConnection
# from amqpstorm import Channel
# from pytest_rabbitmq import factories
#
# rabbitmq_my_proc = factories.rabbitmq_proc(port=5672, logsdir='/tmp')
# rabbitmq_my = factories.rabbitmq('rabbitmq_my_proc')


class MessagingTestCase(unittest.TestCase):
    """Test for Messaging."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch.object(ScalableRpcServer, 'start_server')
    def test_start_server(self, mock):
        """Test if start a server was called."""
        SERVER = ScalableRpcServer()
        SERVER.start_server()
        self.assertTrue(mock.called)

    @patch.object(Consumer, 'start')
    def test_start_consumer(self, mock):
        """Test if start a consumer was called."""
        CONSUMER = Consumer('base.rpc_queue')
        CONSUMER.start()
        self.assertTrue(mock.called)

    @patch.object(Publisher, 'push')
    def test_start_publisher(self, mock):
        """Test if start a Publisher was called."""
        CONSUMER = Publisher()
        CONSUMER.push()
        self.assertTrue(mock.called)
