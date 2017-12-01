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

    def _alias_index_list(self):
        """Retrieve Elasticsearch aliases and their indices."""
        alias_index = dict()
        for key in self._alias_list():
            alias_index[key] = [x for x in self.es.indices.get_alias(name=key)]
        return alias_index

    def _index_create(self, reference_alias, target_index=None):
        """Create a new index if it does not exist."""
        try:
            if target_index is not None and not(self.es.indices.exists(target_index)):
                self.es.indices.create(index=target_index, ignore=400, refresh=True)
            else:
                target_index = "{0}_service_{1}".format(reference_alias, datetime.now().strftime('%Y%m%d_%H%M%S'))
            app_logger.info("New index created: \"{0}\".".format(target_index))
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise
        finally:
            return target_index

    def _index_delete(self, index_list):
        """Delete an index and remove it aliases."""
        for target_index in index_list:
            if self.es.indices.exists(target_index):
                self.es.indices.delete(index=target_index, ignore=[400, 404])
        app_logger.info("Index deleted: \"{0}\".".format(target_index))

    def _bulk_index(self, doc_type, data, target_index):
        """Index one or more documents via the bulk operation."""
        try:
            if doc_type is None:
                doc_type = "General"
            self.es.bulk(index=target_index, doc_type=doc_type, body=data, refresh=True)
            app_logger.info("Bulk Index in Elasticsearch at index: \"{0}\" with type: \"{1}\".".format(target_index, doc_type))
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise

    def _api_index(self, doc_id, doc_type, data, target_index):
        """Index single document from an ."""
        try:
            if doc_type is None:
                doc_type = "General"
            self.es.index(index=target_index, id=doc_id, doc_type=doc_type, body=data, refresh=True)
            app_logger.info("Index document {0} in Elasticsearch at index: \"{1}\" with type: \"{2}\".".format(doc_id, target_index, doc_type))
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise

    def _replace_index(self, new_index, alias_list):
        """Replace an existing index based on an alias."""
        update = dict([('actions', [])])
        old_index = []
        reference_alias = next(iter(alias_list or []), None)
        prefix = "{0}_".format(reference_alias)
        try:
            alias_indexes = self._alias_index_list().get(reference_alias)
            if alias_indexes:
                old_index = [x for x in alias_indexes if x.startswith(prefix)]
            for alias in alias_list:
                update["actions"].append({"add": {"index": new_index, "alias": alias}})
                for elem in old_index:
                    update["actions"].append({"remove": {"index": elem, "alias": alias}})
            self.es.indices.update_aliases(body=json.dumps(update))
            self._index_delete(old_index)
            app_logger.info("Replace index finished.")
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise
        finally:
            return json.dumps(update)

    def _add_alias(self, new_index, alias_list):
        """Add index based on an alias."""
        update = dict([('actions', [])])
        try:
            for alias in alias_list:
                update["actions"].append({"add": {"index": new_index, "alias": alias}})
            self.es.indices.update_aliases(body=json.dumps(update))
            app_logger.info("Add alias finished.")
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise
        finally:
            return json.dumps(update)
