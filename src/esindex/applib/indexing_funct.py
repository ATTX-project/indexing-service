import json
from elasticsearch import Elasticsearch
from esindex.utils.logs import app_logger
from esindex.utils.elastic import elastic


def bulk_index(target_index, doc_type, data):
    """Index JSON-LD graph at specific ES endpoint."""
    es = Elasticsearch([{'host': elastic['host'], 'port': elastic['port']}])
    try:
        if target_index is not None and not(es.indices.exists(target_index)):
            es.indices.create(index=target_index, ignore=400)
        else:
            target_index = "default"
        doc_type if doc_type else doc_type="general"
        es.bulk(index=target_index, doc_type=doc_type, body=json.loads(data))
        app_logger.info("Bulk Index in Elasticsearch at index: \"{0}\" with type: \"{1}\".".format(target_index, resource_type))
    except Exception as error:
        app_logger.error('Something is wrong: {0}'.format(error))
        raise

def replace_index(old_index, new_index, alias):
    """Replace an existing index based on an alias."""
    pass
