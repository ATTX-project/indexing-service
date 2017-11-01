import json
import falcon
from esindex.utils.logs import app_logger


class IndexClass(object):
    """Create Indexing class for API."""

    def on_post(self, req, resp, parsed):
        """Respond on GET request to index endpoint."""
        resp.data = json.dumps(["data"], indent=1, sort_keys=True)
        resp.content_type = 'application/json'
        resp.status = falcon.HTTP_202
        app_logger.info('Creating/POST data to index.')
