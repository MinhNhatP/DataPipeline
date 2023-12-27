import datetime
import json
def create_hierarchy_object(data):
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

def build_hierarchy_structure(input_data):
    
    hierarchy_structure = []
    parent_objects = {}
    
    for item in input_data:
        hierarchy_object = create_hierarchy_object(item)
        
        # Tìm đối tượng cha trong danh sách đã tạo hoặc danh sách đối tượng cha
        parent_name = item["config"]["parent"]
        if parent_name in parent_objects:
            parent_object = parent_objects[parent_name]
        else:
            parent_object = next((obj for obj in hierarchy_structure if obj["object"] == parent_name), None)
        
        # Nếu có đối tượng cha, thêm đối tượng hiện tại vào danh sách con của đối tượng cha
        if parent_object:
            parent_object["children"].append(hierarchy_object)
        else:
            hierarchy_structure.append(hierarchy_object)
        
        # Lưu trữ đối tượng cha vào danh sách để tìm kiếm nhanh hơn
        parent_objects[hierarchy_object["object"]] = hierarchy_object
        
    return hierarchy_structure

# Đầu vào JSON ban đầu
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
            "interval": 70,
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

# Xây dựng cấu trúc hierarchy từ đầu vào JSON
hierarchy_structure = build_hierarchy_structure(input_data)

# In ra cấu trúc hierarchy dưới dạng JSON

print(json.dumps(hierarchy_structure, indent=2))
