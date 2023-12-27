import traceback
from datetime import datetime, timedelta
import pytz
from util import store_data, secret, extract_data, constant
import json


def _add_parent_key_to_children(results, parent_name, child_name):
    parent_key = results["parent"]
    for child in results["result"]:
        if child is None:
            pass
        else:
            child["ee_" + parent_name + "_id"] = parent_key
    return results["result"]


def _extract_children_data(event, url, headers):
    try:
        # name of object in query, need have s at the end
        object_name = constant.MAPPING_CHILD_ITEM.get(event["object"], event["object"])

        query_params = getattr(constant, event["object"].upper()).copy()
        query_params["query"] = query_params["query"].replace("{_id_}", event["parent"])

        time_window = event["time_window"]

        from_time = datetime.strptime(time_window["from"], "%Y-%m-%d %H:%M:%S")
        from_time = from_time - timedelta(days=365 * 2)
        from_time = pytz.utc.localize(from_time).isoformat()

        to_time = datetime.strptime(time_window["to"], "%Y-%m-%d %H:%M:%S")
        to_time = pytz.utc.localize(to_time).isoformat()

        if event.get("endCursor", "") == "":
                query_params["query"] = query_params["query"].replace(
                    'after:"{_end_cursor_}"', event.get("endCursor", "")
                )
        else:
                query_params["query"] = query_params["query"].replace(
                    "{_end_cursor_}", event.get("endCursor", "")
                )

        page_info, results = extract_data.extract_child_response(
            event, url, headers, query_params
        )

        event["hasNextPage"] = page_info["hasNextPage"]
        event["endCursor"] = page_info["endCursor"]

        if results is None:
            event["result"] = None
        
        return event

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Can not extract data with error: ", exception_traceback)
        raise e


def lambda_handler(event, context):
    try:
        user_key = secret.retrieve_credential()
        url, headers = extract_data.build_api_query(event, user_key)
        _extract_children_data(event, url, headers)
        if event["result"] is not None:
            event["result"] = _add_parent_key_to_children(
                event, event["config"]["parent"], event["object"]
            )
            store_data.store_data_in_s3(event)
            parents = event["result"]
            query_key = event["config"].get("query_key", "")
            event["result"] = [
                (parent[query_key]) for parent in parents if parent is not None
            ]
        return event

    except Exception as e:
        print(json.dumps(event))
        raise e
