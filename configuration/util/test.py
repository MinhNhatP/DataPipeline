import datetime
import json
input_data = [
    {
        "object": "organization",
        "config": {
            "type": "hierarchy_snapshot",
            "parent": None,
            "interval": 5,
            "key": "id"
        }
    },
    {
        "object": "repository",
        "config": {
            "type": "hierarchy_snapshot",
            "parent": "organization",
            "interval": 5,
            "key": "id"
        }
    },
    {
        "object": "pullRequest",
        "config": {
            "type": "hierarchy_snapshot",
            "parent": "repository",
            "interval": 5,
            "key": "id"
        }
    },
    {
        "object": "language",
        "config": {
            "type": "hierarchy_snapshot",
            "parent": "repository",
            "interval": 5,
            "key": "id"
        }
    }
]

def create_hierarchy(data):
    object_name = data["object"]
    parent = data["config"]["parent"]
    interval = data["config"]["interval"]
    key = data["config"]["key"]
    type = data["config"]["type"]

    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    to_time = (datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=interval)).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "object": object_name,
        "time_window": {
            "from": current_time,
            "to": to_time
        },
        "config": {
            "extract_type": type,
            "parent": parent,
            "interval_time_to_extract": interval,
            "query_key": key
        },
        "children": []
    }


def build_hierarchy(input_data):

    hierarchy_struct = []
    parent_objects = {}

    for item in input_data:
        hierarchy_object = create_hierarchy(item)

        parent_name = item["config"]["parent"]
        if parent_name in parent_objects:
            parent_object = parent_objects[parent_name]
        else:
            parent_object = next((obj for obj in hierarchy_struct if obj["object"] == parent_name),None)

        if parent_object:
            parent_object["children"].append(hierarchy_object)
        else:
            hierarchy_struct.append(hierarchy_object)
        parent_objects[hierarchy_object["object"]] = hierarchy_object

        print('-------------------')
        print(parent_objects)
    return hierarchy_struct


hierarchy_structure = build_hierarchy(input_data)
# print(json.dumps(hierarchy_structure, indent=2))