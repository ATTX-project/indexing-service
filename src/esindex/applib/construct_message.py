import json
import requests
from esindex.utils.logs import app_logger
from datetime import datetime
from esindex.utils.broker import broker
from esindex.applib.messaging_publish import Publisher
from urlparse import urlparse
from requests_file import FileAdapter
from esindex.applib.elastic_index import ElasticIndex

artifact_id = 'IndexingService'  # Define the IndexingService agent
agent_role = 'index'  # Define Agent type


def replace_message(message_data):
    """Store data in the Graph Store."""
    startTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    aliases = message_data["payload"]["indexingServiceInput"]["targetAlias"]
    alias_list = [str(r) for r in aliases]
    reference_alias = next(iter(alias_list or []), None)
    source_data = message_data["payload"]["indexingServiceInput"]["sourceData"]
    PUBLISHER = Publisher(broker['host'], broker['user'], broker['pass'], broker['provqueue'])
    elastic = ElasticIndex()
    create_indexes = []
    try:
        for source in iter(source_data or []):
            data = retrieve_data(source["inputType"], source["input"])
            new_index = elastic._bulk_index(reference_alias, source["docType"], data)
            elastic._replace_index(new_index, alias_list)
            create_indexes.append(new_index)
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        PUBLISHER.push(prov_message(message_data, "success", startTime, endTime, create_indexes))
        app_logger.info('Replaced Indexed data with: {0} aliases'.format(alias_list))
        return json.dumps(response_message(message_data["provenance"], "success"), indent=4, separators=(',', ': '))
    except Exception as error:
        endTime = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
        PUBLISHER.push(prov_message(message_data, "error", startTime, endTime, create_indexes))
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


def prov_message(message_data, status, startTime, endTime, index_list):
    """Construct GM related provenance message."""
    message = dict()
    message["provenance"] = dict()
    message["provenance"]["agent"] = dict()
    message["provenance"]["agent"]["ID"] = artifact_id
    message["provenance"]["agent"]["role"] = agent_role

    activityID = message_data["provenance"]["context"]["activityID"]
    workflowID = message_data["provenance"]["context"]["workflowID"]

    prov_message = message["provenance"]

    prov_message["context"] = dict()
    prov_message["context"]["activityID"] = str(activityID)
    prov_message["context"]["workflowID"] = str(workflowID)
    if message_data["provenance"]["context"].get('stepID'):
        prov_message["context"]["stepID"] = message_data["provenance"]["context"]["stepID"]

    prov_message["activity"] = dict()
    prov_message["activity"]["type"] = "ServiceExecution"
    prov_message["activity"]["title"] = "Indexing Service Operations."
    prov_message["activity"]["status"] = status
    prov_message["activity"]["startTime"] = startTime
    prov_message["activity"]["endTime"] = endTime
    message["provenance"]["input"] = []
    message["provenance"]["output"] = []
    message["payload"] = {}
    for index in index_list:
        output_data = {
            "index": index,
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

    app_logger.info('Construct provenance metadata for Indexing Service.')
    print json.dumps(message)
    return json.dumps(message)


def response_message(provenance_data, output):
    """Construct Graph Manager response."""
    message = dict()
    message["provenance"] = dict()
    message["provenance"]["agent"] = dict()
    message["provenance"]["agent"]["ID"] = artifact_id
    message["provenance"]["agent"]["role"] = agent_role

    activityID = provenance_data["context"]["activityID"]
    workflowID = provenance_data["context"]["workflowID"]

    prov_message = message["provenance"]

    prov_message["context"] = dict()
    prov_message["context"]["activityID"] = str(activityID)
    prov_message["context"]["workflowID"] = str(workflowID)
    if provenance_data["context"].get('stepID'):
        prov_message["context"]["stepID"] = provenance_data["context"]["stepID"]
    message["payload"] = dict()
    message["payload"]["indexingServiceOutput"] = output
    return message
