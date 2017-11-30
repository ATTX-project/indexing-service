import falcon
from esindex.utils.logs import app_logger
from esindex.api.healthcheck import HealthCheck
from esindex.api.indexing import AliasesList, IndexClass

api_version = "0.2"  # TO DO: Figure out a better way to do versioning


def init_api():
    """Create the API endpoint."""
    indexservice = falcon.API()

    indexservice.add_route('/health', HealthCheck())

    indexservice.add_route('/%s/alias/list' % (api_version), AliasesList())

    indexservice.add_route('/%s/data/index' % (api_version), IndexClass())

    app_logger.info('IndexService REST API is running.')
    return indexservice


# if __name__ == '__main__':
#     init_api()
