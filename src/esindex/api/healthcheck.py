import json
import falcon
from esindex.utils.logs import app_logger
from esindex.utils.elastic import elastic
from elasticsearch import Elasticsearch

# TO DO More detailed response from the health endpoint with statistics
# For now the endpoint responds with a simple 200


def healthcheck_response(api_status):
    """Content and format health status response."""
    health_status = dict([('indexService', api_status)])
    es = Elasticsearch([{'host': elastic['host'], 'port': elastic['port']}])
    if es.ping():
        health_status['elasticsearch'] = "Running"
        # health_status['elasticsearch'] = {"status": es.cluster.health().get('status')}
    else:
        health_status['elasticsearch'] = "Not Running"
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
