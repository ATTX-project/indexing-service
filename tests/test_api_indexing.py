import unittest
import responses
import falcon
# import json
from falcon import testing
from esindex.app import init_api
from mock import patch
from esindex.applib.elastic_index import ElasticIndex


class IndexingAPITestCase(testing.TestCase):
    """Testing Queues task."""

    def setUp(self):
        """Setting the app up."""
        self.api = "http://localhost:4304/"
        self.version = "0.2"
        self.app = init_api()

    def tearDown(self):
        """Tearing down the app up."""
        pass


class IndexingTestCase(IndexingAPITestCase):
    """Test for Indexing API operations."""

    def test_create(self):
        """Test create message."""
        self.app
        pass

    @responses.activate
    @patch.object(ElasticIndex, '_alias_list')
    def test_api_alias_list_get(self, mock):
        """Test alias list endpoint."""
        responses.add(responses.GET, "{0}{1}/alias/list".format(self.api, self.version), status=200)
        result = self.simulate_get("/{0}/alias/list".format(self.version))
        self.assertTrue(mock.called)
        assert(result.status == falcon.HTTP_200)

    @responses.activate
    def test_api_index_no_data(self):
        """Test index validate no data."""
        hdrs = [('Accept', 'application/json'),
                ('Content-Type', 'application/json'), ]
        result = self.simulate_post('/{0}/data/index'.format(self.version), body='', headers=hdrs)
        assert(result.status == falcon.HTTP_400)

    @responses.activate
    def test_api_index_bad(self):
        """Test index bad input."""
        with open('tests/resources/request_bad_index.json') as datafile:
            index_message = datafile.read().replace('\n', '')
        hdrs = [('Accept', 'application/json'),
                ('Content-Type', 'application/json'), ]
        result = self.simulate_post('/{0}/data/index'.format(self.version), body=index_message, headers=hdrs)
        assert(result.status == falcon.HTTP_400)

    @responses.activate
    @patch('esindex.api.indexing.add_message')
    def test_api_index_post(self, mock):
        """Test get Accept from indexing data."""
        hdrs = [('Accept', 'application/json'),
                ('Content-Type', 'application/json'), ]
        with open('tests/resources/request_index.json') as datafile:
            index_message = datafile.read().replace('\n', '')
        mock.return_value = '{}'
        responses.add(responses.POST, "{0}{1}/data/index".format(self.api, self.version), status=200)
        result = self.simulate_post("/{0}/data/index".format(self.version), body=index_message, headers=hdrs)
        assert(result.status == falcon.HTTP_202)


if __name__ == "__main__":
    unittest.main()
