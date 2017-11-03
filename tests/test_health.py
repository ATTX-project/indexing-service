# import falcon
import unittest
# import httpretty
# import json
from falcon import testing
from esindex.app import init_api
# from esindex.applib.graph_store import GraphStore
# from esindex.api.healthcheck import healthcheck_response
# from mock import patch


class appHealthTest(testing.TestCase):
    """Testing GM prov function and initialize it for that purpose."""

    def setUp(self):
        """Setting the app up."""
        self.app = init_api()

    def tearDown(self):
        """Tearing down the app up."""
        pass


class TestProv(appHealthTest):
    """Testing if there is a health endoint available."""

    def test_create(self):
        """Test GET health message."""
        self.app
        pass

    # @httpretty.activate
    # def test_health_ok(self):
    #     """Test GET health is ok."""
    #     httpretty.register_uri(httpretty.GET, "http://localhost:4032/health", status=200)
    #     result = self.simulate_get('/health')
    #     assert(result.status == falcon.HTTP_200)
    #
    # @httpretty.activate
    # def test_health_response(self):
    #     """Response to healthcheck endpoint."""
    #     fuseki = GraphStore()
    #     httpretty.register_uri(httpretty.GET, "http://localhost:3030/{0}/ping".format("$"), "2017-09-18T11:41:19.915+00:00", status=200)
    #     httpretty.register_uri(httpretty.GET, "http://localhost:7030/health", status=200)
    #     response = healthcheck_response("Running", fuseki)
    #     result = self.simulate_get('/health')
    #     assert(result.content == response)
    #
    # @patch('graph_manager.api.healthcheck.healthcheck_response')
    # def test_actual_health_response(self, mock):
    #     """Test if json response format."""
    #     fuseki = GraphStore()
    #     mock.return_value = {"graphStore": "Not Running", "graphManagerService": "Running", "messageBroker": "Not Running"}
    #     response = healthcheck_response("Running", fuseki)
    #     json_response = {"graphStore": "Not Running", "graphManagerService": "Running", "messageBroker": "Not Running"}
    #     assert(json_response == json.loads(response))


if __name__ == "__main__":
    unittest.main()
