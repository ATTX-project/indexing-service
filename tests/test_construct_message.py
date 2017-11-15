import json
import unittest
from esindex.applib.construct_message import replace_message, add_message
from mock import patch
from esindex.applib.elastic_index import ElasticIndex
from amqpstorm import AMQPConnectionError


class ConstructFrameTestCase(unittest.TestCase):
    """Test for constuct message frame function."""

    def setUp(self):
        """Set up test fixtures."""
        pass

    @patch('esindex.applib.construct_message.Publisher.push')
    @patch.object(ElasticIndex, '_bulk_index')
    @patch.object(ElasticIndex, '_replace_index')
    def test_replace_called(self, mock, mock_bulk, publish_mock):
        """Test if replace index was called."""
        with open('tests/resources/request_index.json') as datafile:
            message = json.load(datafile)
        # mock.return_value = result_data
        replace_message(message)
        self.assertTrue(mock.called)

    @patch('esindex.applib.construct_message.Publisher.push')
    @patch.object(ElasticIndex, '_bulk_index')
    @patch.object(ElasticIndex, '_add_alias')
    def test_add_called(self, mock, mock_bulk, publish_mock):
        """Test if add alias was called."""
        with open('tests/resources/request_index.json') as datafile:
            message = json.load(datafile)
        # mock.return_value = result_data
        add_message(message)
        self.assertTrue(mock.called)

    @patch.object(ElasticIndex, '_replace_index')
    @patch.object(ElasticIndex, '_bulk_index')
    def test_ld_error(self, mock, mock_replace):
        """Test if replace raises an error was called."""
        with open('tests/resources/request_index.json') as datafile:
            message = json.load(datafile)
        with self.assertRaises(AMQPConnectionError):
            replace_message(message)


if __name__ == "__main__":
    unittest.main()
