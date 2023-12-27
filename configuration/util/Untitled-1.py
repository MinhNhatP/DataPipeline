import json
import pprint

data = {'object': 'organization', 'time_window': {'from': '2023-09-09 02:20:00', 'to': '2023-09-09 02:25:00'}, 'config': {'extract_type': 'hierarchy_snapshot', 'parent': None, 'interval_time_to_extract': 5, 'query_key': 'id'}, 'children': []}, {'object': 'repository', 'time_window': {'from': '2023-09-09 02:20:00', 'to': '2023-09-09 02:25:00'}, 'config': {'extract_type': 'hierarchy_snapshot', 'parent': None, 'interval_time_to_extract': 5, 'query_key': 'id'}, 'children': []}, {'object': 'pullRequest', 'time_window': {'from': '2023-09-09 02:20:00', 'to': '2023-09-09 02:25:00'}, 'config': {'extract_type': 'hierarchy_snapshot', 'parent': None, 'interval_time_to_extract': 5, 'query_key': 'id'}, 'children': []}, {'object': 'language', 'time_window': {'from': '2023-09-09 02:20:00', 'to': '2023-09-09 02:25:00'}, 'config': {'extract_type': 'hierarchy_snapshot', 'parent': None, 'interval_time_to_extract': 5, 'query_key': 'id'}, 'children': []}

pprint.pprint(data)