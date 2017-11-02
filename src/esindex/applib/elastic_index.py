import json
from os import environ
from datetime import datetime
from elasticsearch import Elasticsearch
from esindex.utils.logs import app_logger


class ElasticIndex(object):
    """Handle requests to the Elasticsearch Endpoint."""

    def __init__(self):
        """Check if we have everything to work with the Graph Store."""
        self.host = environ['ESHOST'] if 'ESHOST' in environ else "localhost"
        self.port = environ['ESPORT'] if 'ESPORT' in environ else "9210"
        self.otherport = environ['ESPORTB'] if 'ESPORTB' in environ else "9310"

        self.es = Elasticsearch([{'host': self.host, 'port': self.port}])

    def _healthcheck(self):
        """Retrieve Elasticsearch health."""
        health = dict()
        if self.es.ping():
            health["status"] = "Running",
            health["loadStatus"] = self.es.cluster.health().get('status')
        else:
            health["status"] = "Not Running"
        return health

    def _alias_list(self, index=None, alias=None):
        """Retrieve Elasticsearch aliases list."""
        aliases = set()
        for key, alias in self.es.indices.get_alias(index=index, name=alias).iteritems():
            map(lambda x: aliases.add(x), [x for x in alias.get("aliases")])
        return aliases

    def _alias_index_list(self, index=None, alias=None):
        """Retrieve Elasticsearch aliases and their indices."""
        alias_index = self._alias_list()
        for key in alias_index:
            alias_index[key] = [x for x in self.es.indices.get_alias(name=key)]
        return alias_index

    def _bulk_index(self, target_alias, doc_type, data, target_index=None):
        """Index JSON-LD graph at specific ES endpoint."""
        try:
            if target_index is not None and not(self.es.indices.exists(target_index)):
                self.es.indices.create(index=target_index, ignore=400)
            else:
                target_index = "{0}_service_{1}".format(target_alias, datetime.now().strftime('%Y%m%d_%H%M%S'))
            if doc_type is None:
                doc_type = "General"
            self.es.bulk(index=target_index, doc_type=doc_type, body=data)
            app_logger.info("Bulk Index in Elasticsearch at index: \"{0}\" with type: \"{1}\".".format(target_alias, doc_type))
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise
        finally:
            return target_index

    def _replace_index(self, new_index, alias):
        """Replace an existing index based on an alias."""
        update = dict([('actions', [])])
        prefix = "{0}_".format(alias)
        update["actions"].add({"add": {"index": new_index, "alias": alias}})
        filter(lambda x: x.startswith(prefix), self._alias_index_list.get(alias))
        self.es.indices.update_aliases(body=json.dumps(update))
