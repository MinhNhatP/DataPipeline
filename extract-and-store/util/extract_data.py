import os
import json
import traceback

import requests
from requests.exceptions import HTTPError

#from util 
import constant


def build_api_query(event, user_key):
    try:
        # url = base_url + event["config"]["url_path"]
        url = "https://api.github.com/graphql"
        headers = {"Accept": "application/json", "Authorization": user_key}
        return url, headers
    except Exception as e:
        exception_traceback = traceback.format_exc()
        print(
            "An error occurs while building query for extracting data from New Relic: ",
            exception_traceback,
        )
        raise e


def _get_from_graphql(url, headers, params):
    response = requests.request("POST", url, headers=headers, json=params)
    status_code = response.status_code
    if response.ok:
        body = response.json()
    else:
        raise HTTPError(
            f"An error occurs when calling the server's REST API. "
            f"Server returns the HTTP code {status_code} instead of 200 OK."
        )
    return body


def convert_tools(data):
    if not data:
        return None
    if isinstance(data, list):
        return [convert_tools(item) for item in data]
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if key == "edges":
                result = convert_tools(value)
            elif isinstance(value, list):
                result[key] = convert_tools(value)
            elif isinstance(value, dict):
                if "node" in value:
                    result = convert_tools(value["node"])
                else:
                    result[key] = convert_tools(value)
            else:
                result[key] = value
        return result


def move_node_value_to_parent(json_obj, level=1):
    while level <= 6:
        if isinstance(json_obj, dict):
            found_node_key = False
            for k, v in json_obj.copy().items():
                if k == "node":
                    # Move the node value to the parent level
                    json_obj.update(v)
                    del json_obj[k]
                    found_node_key = True
                else:
                    move_node_value_to_parent(v, level + 1)
            if found_node_key:
                continue
        elif isinstance(json_obj, list):
            for item in json_obj:
                move_node_value_to_parent(item, level + 1)
        level += 1
    return json_obj


def extract_parent_response(object_api_name, url, headers, query_params):
    try:
        response = _get_from_graphql(url, headers, query_params)
        page_info = response["data"][object_api_name]["pageInfo"]
        results = move_node_value_to_parent(response)
        return page_info, results
    except Exception as e:
        print("Can not extract data with error: ", traceback.format_exc())
        raise e


def extract_org_response(object_api_name, url, headers, query_params):
    try:
        response = _get_from_graphql(url, headers, query_params)
        page_info = {}
        return page_info, response
    except Exception as e:
        print("Can not extract data with error: ", traceback.format_exc())
        raise e


def extract_child_response(event, url, headers, query_params):
    try:
        response = _get_from_graphql(url, headers, query_params)
        # name of object in query, need have s at the end
        object_name = constant.MAPPING_CHILD_ITEM.get(event["object"], event["object"])
        if event["object"] == "commit":
            page_info = response["data"]["node"]["target"]["history"]["pageInfo"]
            total_count = response["data"]["node"]["target"]["history"]["totalCount"]
        else:
            page_info = response["data"]["node"][object_name]["pageInfo"]
            total_count = response["data"]["node"][object_name]["totalCount"]
        if total_count <= 0:
            results = None
        else:
            results = response
        return page_info, results
    except Exception as e:
        print("Can not extract data with error: ", traceback.format_exc())
        raise e
