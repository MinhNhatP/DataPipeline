import traceback
from util import store_data, secret, extract_data, constant
from util.generate_log_url import logging_url
import json


def _add_parent_id_to_children(results, parent_name, child_name):
    for parent in results:
        parent_id = parent["id"]
        for child in parent[child_name]:
            child[parent_name + ".id"] = parent_id
    return results


def _extract_and_store_children_from_a_parent(event):
    event = event.pop("children")
    return event


def _extract_parent_data(event, url, headers):
    try:
        query_params = getattr(constant, event["object"].upper()).copy()
        if event.get("endCursor", "") == "":
            query_params["query"] = query_params["query"].replace(
                'after:"{_end_cursor_}"', event.get("endCursor", "")
            )
        else:
            query_params["query"] = query_params["query"].replace(
                "{_end_cursor_}", event.get("endCursor", "")
            )

        object_api_name = constant.MAPPING_API_NAME.get(
            event["object"], f"{event['object']}s"
        )
        page_info, results = extract_data.extract_parent_response(
            object_api_name, url, headers, query_params
        )
        event.update(page_info)
        event["result"] = results["data"][object_api_name]["nodes"]
        return event

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Can not extract data with error: ", exception_traceback)
        raise e


def _extract_organization_data(event, url, headers):
    try:
        query_params = getattr(constant, event["object"].upper()).copy()
        object_api_name = constant.MAPPING_API_NAME.get(
            event["object"], f"{event['object']}"
        )
        page_info, results = extract_data.extract_org_response(
            object_api_name, url, headers, query_params
        )
        event["result"] = [results["data"][object_api_name]]
        return event

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Can not extract data with error: ", exception_traceback)
        raise e


def lambda_handler(event, context):
    try:
        user_key = secret.retrieve_credential()
        url, headers = extract_data.build_api_query(event, user_key)
        if event["object"] == "organization":
            data = [
                {"id": "MDEyOk9yZ2FuaXphdGlvbjYxNTQ3MjI=", "name": "Microsoft"},
                {"id": "MDEyOk9yZ2FuaXphdGlvbjEzNDIwMDQ=", "name": "Google"},
            ]
            event["result"] = data
            event["hasNextPage"] = False
            result = event
        else:
            result = _extract_parent_data(event, url, headers)
        store_data.store_data_in_s3(result)
        parents = result["result"]
        # TODO: Filter Parent id
        query_key = event["config"].get("query_key", "")
        result["result"] = [
            (parent[query_key]) for parent in parents if parent is not None
        ]
        return result
    except Exception as e:
        raise e
