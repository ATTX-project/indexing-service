import falcon
from esindex.utils.logs import app_logger
from esindex.api.healthcheck import HealthCheck

api_version = "0.2"  # TO DO: Figure out a better way to do versioning


def init_api():
    """Create the API endpoint."""
    indexservice = falcon.API()

    indexservice.add_route('/health', HealthCheck())

    app_logger.info('IndexService REST API is running.')
    return indexservice


# if __name__ == '__main__':
#     init_api()
