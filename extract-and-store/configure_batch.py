import os
import copy
import datetime
from datetime import timedelta
import traceback

from util import extraction_config

DATETIME_FORMAT  =  "%Y-%m-%d %H:%M:%S"
MINIMUM_DELTA_TIME = 5

# DATETIME_FORMAT = os.getenv("DATETIME_FORMAT")
# MINIMUM_DELTA_TIME = int(os.getenv("MINIMUM_DELTA_TIME"))


def _get_extraction_configuration(event):
    if "object" in event:
        data_extraction_config = extraction_config.get_config_for_all_objects()
    else:
        list_objects = list(event["object"])
        data_extraction_config = extraction_config.get_config_for_a_list_of_objects(
            list_objects
        )
    return data_extraction_config


def _find_the_end_time_of_current_window():
    try:
        current = datetime.datetime.now()
        new_minute = current.minute // MINIMUM_DELTA_TIME
        to_date = current.replace(
            minute=new_minute * MINIMUM_DELTA_TIME, second=0, microsecond=0
        )
        to_d = to_date.strftime(DATETIME_FORMAT)
        return to_d, to_date.minute

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Fail to find the end time of current window: ", exception_traceback)
        raise


def _determine_objects_need_to_be_extracted_in_the_current_window(
    data_extraction_config, minute_of_current_window
):
    objects_need_to_be_extracted_in_the_current_window = list(
        filter(
            lambda x: minute_of_current_window % x["config"]["interval_time_to_extract"]
            == 0,
            data_extraction_config,
        )
    )
    return objects_need_to_be_extracted_in_the_current_window


def generate_hierarchy(parent_children_pair, parent_objects_config):
    hierarchy = []
    parent_child_dict = {}
    for parent, children in parent_children_pair.items():
        if parent not in parent_child_dict:
            parent_child_dict[parent] = []
        parent_child_dict[parent].extend(children)

    for parent_obj in parent_objects_config:
        obj = copy.deepcopy(parent_obj)
        obj["children"] = build_children(obj["object"], parent_child_dict)
        hierarchy.append(obj)
    return hierarchy


def build_children(parent, parent_child_dict):
    children = []

    if parent in parent_child_dict:
        for child in parent_child_dict[parent]:
            child_obj = copy.deepcopy(child)
            child_obj["children"] = build_children(
                child_obj["object"], parent_child_dict
            )
            children.append(child_obj)
    return children


def _determine_parent_children_objects(data_extraction_config):
    parent_children_pair = {}
    parent_objects_config = []
    for config in data_extraction_config:
        parent_name = config["config"]["parent"]
        if parent_name is not None:
            if parent_name in parent_children_pair:
                parent_children_pair[parent_name].append(
                    {
                        "object": config["object"],
                        "time_window": config["time_window"],
                        "config": config["config"],
                    }
                )
            else:
                parent_children_pair[parent_name] = [
                    {
                        "object": config["object"],
                        "time_window": config["time_window"],
                        "config": config["config"],
                    }
                ]
        else:
            parent_objects_config.append(config)

    return generate_hierarchy(parent_children_pair, parent_objects_config)



def _configure_manual_batches(event, config):
    if len(config) == 1:
        conf = config[0]

        batch = {
            "object": conf["object"],
            "time_window": {
                "from": event["time_window"]["from"],
                "to": event["time_window"]["to"],
            },
            "config": conf["config"],
        }
        if "parent" in event:
            batch["parent"] = event["parent"]

        return [batch]
    else:
        raise Exception("Can not find configuration for event: ", event)


def _configure_automatic_batches(data_extraction_config):
    try:
        batches = []
        for config in data_extraction_config:
            from_d, to_d = _get_window_time(config)
            batches.append(
                {
                    "object": config["object"],
                    "time_window": {"from": from_d, "to": to_d},
                    "config": config["config"],
                }
            )
        return list(batches)
    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Can not get output with error: ", exception_traceback)
        raise


def _get_window_time(config):
    try:
        current = datetime.datetime.now()
        delta_time = config["config"]["interval_time_to_extract"]
        new_minute = current.minute // delta_time
        to_date = current.replace(
            minute=new_minute * delta_time, second=0, microsecond=0
        )
        from_date = to_date - timedelta(minutes=delta_time)
        to_d = to_date.strftime(DATETIME_FORMAT)
        from_d = from_date.strftime(DATETIME_FORMAT)
        return from_d, to_d

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Fail to define windows times: ", exception_traceback)
        raise


def _is_manual_extracting(event):
    if "object" in event and "time_window" in event:
        return True
    elif event == {}:
        return False
    else:
        raise Exception("Invalid input: ", event)


def lambda_handler(event, context):
    try:
        data_extraction_config = extraction_config.get_config_for_all_objects()
        if _is_manual_extracting(event):
            config = list(
                filter(lambda x: x["object"] == event["object"], data_extraction_config)
            )
            batches = _configure_manual_batches(event, config)
        else:
            (
                the_end_time_of_current_window,
                minute_of_current_window,
            ) = _find_the_end_time_of_current_window()
            data_extraction_config = (
                _determine_objects_need_to_be_extracted_in_the_current_window(
                    data_extraction_config, minute_of_current_window
                )
            )
            batches = _configure_automatic_batches(data_extraction_config)
            batches = _determine_parent_children_objects(batches)
        return batches

    except Exception as e:
        raise e
