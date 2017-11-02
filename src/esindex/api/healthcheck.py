import json
import falcon
from esindex.utils.logs import app_logger
from esindex.applib.elastic_index import ElasticIndex

# TO DO More detailed response from the health endpoint with statistics
# For now the endpoint responds with a simple 200


def healthcheck_response(api_status):
    """Content and format health status response."""
    elastic = ElasticIndex()
    health_status = dict([('indexService', api_status)])
    health_status['elasticsearch'] = ''.join(elastic._healthcheck().get("status"))
    return json.dumps(health_status, indent=1, sort_keys=True)


class HealthCheck(object):
    """Create HealthCheck class."""

    def on_get(self, req, resp):
        """Respond on GET request to map endpoint."""
        # if you manange to call this it means the API is running
        resp.data = healthcheck_response("Running")
        resp.content_type = 'application/json'
        resp.status = falcon.HTTP_200
        app_logger.info('Finished operations on /health GET Request.')
