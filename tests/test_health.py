import falcon
import unittest
import httpretty
import json
from falcon import testing
from esindex.app import init_api
from esindex.api.healthcheck import healthcheck_response
from mock import patch
# import responses


class appHealthTest(testing.TestCase):
    """Testing GM prov function and initialize it for that purpose."""

    def setUp(self):
        """Setting the app up."""
        self.app = init_api()

    def tearDown(self):
        """Tearing down the app up."""
        pass


class TestIndex(appHealthTest):
    """Testing if there is a health endoint available."""

    def test_create(self):
        """Test GET health message."""
        self.app
        pass

    @httpretty.activate
    def test_health_ok(self):
        """Test GET health is ok."""
        httpretty.register_uri(httpretty.GET, "http://localhost:4304/health", status=200)
        result = self.simulate_get('/health')
        assert(result.status == falcon.HTTP_200)
        httpretty.disable()
        httpretty.reset()

    @httpretty.activate
    def test_health_response(self):
        """Response to healthcheck endpoint."""
        httpretty.register_uri(httpretty.GET, "http://user:password@localhost:15672/api/aliveness-test/%2F", body='{"status": "ok"}', status=200)
        # httpretty.register_uri(httpretty.HEAD, "http://localhost:9210", status=200)
        # httpretty.register_uri(httpretty.GET, "http://localhost:9210/_cluster/health", body='{"status": "green"}', status=200)
        httpretty.register_uri(httpretty.GET, "http://localhost:4304/health", status=200)
        response = healthcheck_response("Running")
        result = self.simulate_get('/health')
        json_response = {"indexService": "Running", "messageBroker": "Running", "elasticsearch": "Not Running"}
        assert(json_response == json.loads(response))
        assert(result.content == response)
        httpretty.disable()
        httpretty.reset()
    # 
    # @patch('esindex.api.healthcheck.healthcheck_response')
    # def test_actual_health_response(self, mock):
    #     """Test if json response format."""
    #     mock.return_value = {"indexService": "Running", "messageBroker": "Not Running", "elasticsearch": "Not Running"}
    #     response = healthcheck_response("Running")
    #     json_response = {"indexService": "Running", "messageBroker": "Not Running", "elasticsearch": "Not Running"}
    #     assert(json_response == json.loads(response))


if __name__ == "__main__":
    unittest.main()
