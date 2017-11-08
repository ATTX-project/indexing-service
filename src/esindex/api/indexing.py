import json
import falcon
from esindex.utils.logs import app_logger
from esindex.applib.elastic_index import ElasticIndex
from esindex.schemas import load_schema
from esindex.utils.validate import validate
from esindex.applib.construct_message import add_message


class IndexClass(object):
    """Create Indexing class for API."""

    @validate(load_schema('index'))
    def on_post(self, req, resp, parsed):
        """Respond on GET request to index endpoint."""
        resp.data = add_message(parsed)
        resp.content_type = 'application/json'
        resp.status = falcon.HTTP_202
        app_logger.info('Index/POST data with specified aliases.')


class AliasesList(object):
    """List named graphs in the graph store."""

    def on_get(self, req, resp):
        """Execution of the GET aliases list request."""
        elastic = ElasticIndex()
        resp.data = json.dumps(list(elastic._alias_list()), indent=1, sort_keys=True)
        resp.content_type = 'application/json'
        resp.status = falcon.HTTP_200
        app_logger.info('Finished operations on /aliases/list GET Request.')
