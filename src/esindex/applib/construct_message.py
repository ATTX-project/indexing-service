import json
import requests
from esindex.utils.logs import app_logger
from datetime import datetime
from esindex.utils.broker import broker
from esindex.applib.messaging_publish import Publisher
from urlparse import urlparse
from requests_file import FileAdapter
from esindex.applib.elastic_index import ElasticIndex
from functools import wraps

artifact_id = "IndexingService"  # Define the IndexingService agent
agent_role = "index"  # Define Agent type
output_key = "indexingServiceOutput"


def index_data(func):
    """Decorator function."""
    @wraps(func)
    def wrapper(message_data, *args, **kwargs):
        """Wrap it nicely."""
        aliases = message_data["payload"]["indexingServiceInput"]["targetAlias"]
        alias_list = [str(r) for r in aliases]
        # If there are multiple aliases in the message we need the first one for reference.
        # The reference alias will act as a unique identified for the index to be replaced.
        reference_alias = next(iter(alias_list or []), None)
        source_data = message_data["payload"]["indexingServiceInput"]["sourceData"]
        elastic = ElasticIndex()
        # We will be using only one index for all data sources.
        replace_index = elastic._index_create(reference_alias)
        try:
            for source in iter(source_data or []):
                data = retrieve_data(source["inputType"], source["input"])
                if source["useBulk"] and source["useBulk"] is True:
                    elastic._bulk_index(source["docType"], data, replace_index)
                elif source["documentID"]:
                    elastic._api_index(source["documentID"], source["docType"], data, replace_index)
                else:
                    raise KeyError("Missing useBulk operation or documentID for single document indexing.")
            return func(message_data, replace_index=replace_index, alias_list=alias_list)
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise
    return wrapper


@index_data
def replace_message(message_data, replace_index, alias_list):
    """Replace an old index with a new index for a given alias list."""
    startTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    PUBLISHER = Publisher(broker['host'], broker['user'], broker['pass'], broker['provqueue'])
    elastic = ElasticIndex()
    try:
        # This is what makes it a replace operation.
        elastic._replace_index(replace_index, alias_list)
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        if bool(message_data["provenance"]):
            PUBLISHER.push(prov_message(message_data, "success", startTime, endTime, replace_index))
        app_logger.info('Replaced Indexed data with: {0} aliases'.format(alias_list))
        return json.dumps(response_message(message_data["provenance"], status="success"), indent=4, separators=(',', ': '))
    except Exception as error:
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        PUBLISHER.push(prov_message(message_data, "error", startTime, endTime, replace_index))
        app_logger.error('Something is wrong: {0}'.format(error))
        raise


@index_data
def add_message(message_data, replace_index, alias_list):
    """Index data and associate aliases to the indexes."""
    startTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    PUBLISHER = Publisher(broker['host'], broker['user'], broker['pass'], broker['provqueue'])
    elastic = ElasticIndex()
    try:
        elastic._add_alias(replace_index, alias_list)
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        if bool(message_data["provenance"]):
            PUBLISHER.push(prov_message(message_data, "success", startTime, endTime, replace_index))
        app_logger.info('Indexed data with: {0} aliases'.format(alias_list))
        return json.dumps(response_message(message_data["provenance"], status="success"), indent=4, separators=(',', ': '))
    except Exception as error:
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        PUBLISHER.push(prov_message(message_data, "error", startTime, endTime, replace_index))
        app_logger.error('Something is wrong: {0}'.format(error))
        raise


def handle_fileAdapter(request, input_data):
    """Handle file adapter response."""
    if request.status_code == 404:
        raise IOError("Something went wrong with retrieving the file: {0}. It does not exist!".format(input_data))
    elif request.status_code == 403:
        raise IOError("Something went wrong with retrieving the file: {0}. Accessing it is not permitted!".format(input_data))
    elif request.status_code == 400:
        raise IOError("Something went wrong with retrieving the file: {0}. General IOError!".format(input_data))
    elif request.status_code == 200:
        return request.text


def retrieve_data(inputType, input_data):
    """Retrieve data from a specific URI."""
    s = requests.Session()
    allowed = ('http', 'https', 'ftp')
    local = ('file')
    if inputType == "Data":
        return input_data
    elif inputType == "URI":
        try:
            if urlparse(input_data).scheme in allowed:
                request = s.get(input_data, timeout=1)
                return request.text
            elif urlparse(input_data).scheme in local:
                s.mount('file://', FileAdapter())
                request = s.get(input_data)
                return handle_fileAdapter(request, input_data)
        except Exception as error:
            app_logger.error('Something is wrong: {0}'.format(error))
            raise


def prov_message(message_data, status, start_time, end_time, replace_index):
    """Construct Indexing related provenance message."""
    message = dict()
    message["provenance"] = dict()
    message["provenance"]["agent"] = dict()
    message["provenance"]["agent"]["ID"] = artifact_id
    message["provenance"]["agent"]["role"] = agent_role

    activity_id = message_data["provenance"]["context"]["activityID"]
    workflow_id = message_data["provenance"]["context"]["workflowID"]

    prov_message = message["provenance"]

    prov_message["context"] = dict()
    prov_message["context"]["activityID"] = str(activity_id)
    prov_message["context"]["workflowID"] = str(workflow_id)
    if message_data["provenance"]["context"].get('stepID'):
        prov_message["context"]["stepID"] = message_data["provenance"]["context"]["stepID"]

    prov_message["activity"] = dict()
    prov_message["activity"]["type"] = "ServiceExecution"
    prov_message["activity"]["title"] = "Indexing Service Operations."
    prov_message["activity"]["status"] = status
    prov_message["activity"]["startTime"] = start_time
    prov_message["activity"]["endTime"] = end_time
    message["provenance"]["input"] = []
    message["provenance"]["output"] = []
    message["payload"] = {}
    if type(replace_index) is list:
        for index in replace_index:
            output_data = {
                "index": index,
                "key": "outputIndex",
                "role": "Dataset"
            }
            message["provenance"]["output"].append(output_data)
    else:
        output_data = {
            "index": str(replace_index),
            "key": "outputIndex",
            "role": "Dataset"
        }
        message["provenance"]["output"].append(output_data)

    alias_list = [str(r) for r in message_data["payload"]["indexingServiceInput"]["targetAlias"]]
    source_data = message_data["payload"]["indexingServiceInput"]["sourceData"]

    for elem in source_data:
        key = "index_{0}".format(source_data.index(elem))
        input_data = {
            "aliases": alias_list,
            "key": key,
            "role": "alias"
        }
        if elem["inputType"] == "Data":
            message["payload"][key] = "attx:tempDataset"
        if elem["inputType"] == "URI":
            message["payload"][key] = elem["input"]
        message["provenance"]["input"].append(input_data)
    message["payload"]["aliases"] = message_data["payload"]["indexingServiceInput"]["targetAlias"]
    message["payload"]["outputIndex"] = replace_index

    app_logger.info('Construct provenance metadata for Indexing Service.')
    return json.dumps(message)


def response_message(provenance_data, status, status_messsage=None, output=None):
    """Construct Graph Manager response."""
    message = dict()
    message["provenance"] = dict()

    if bool(provenance_data):
        message["provenance"]["agent"] = dict()
        message["provenance"]["agent"]["ID"] = artifact_id
        message["provenance"]["agent"]["role"] = agent_role

        activity_id = provenance_data["context"]["activityID"]
        workflow_id = provenance_data["context"]["workflowID"]

        prov_message = message["provenance"]

        prov_message["context"] = dict()
        prov_message["context"]["activityID"] = str(activity_id)
        prov_message["context"]["workflowID"] = str(workflow_id)
        if provenance_data["context"].get('stepID'):
            prov_message["context"]["stepID"] = provenance_data["context"]["stepID"]
    message["payload"] = dict()
    message["payload"]["status"] = status
    if status_messsage:
        message["payload"]["statusMessage"] = status_messsage
    if output:
        message["payload"][output_key] = output
    return message
