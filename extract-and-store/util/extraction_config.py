import json
import traceback


def get_config_for_all_objects():
    try:
        with open("./config/data_extraction_config.json") as f:
            config = json.load(f)
            return config

    except Exception as e:
        print(
            "An error occurs while getting data extraction configuration: ",
            traceback.format_exc(),
        )
        raise e


def get_config_for_a_list_of_objects(objects):
    try:
        with open("./config/data_extraction_config.json") as f:
            config = json.load(f)

            config_for_a_list_of_objects = filter(
                lambda x: x["object"] in objects, config
            )
            return config_for_a_list_of_objects
    except Exception as e:
        print(
            "An error occurs while getting data extraction configuration: ",
            traceback.format_exc(),
        )
        raise e


def get_interval_time_to_extract_of_an_object(object_name):
    try:
        with open("./config/data_extraction_config.json") as f:
            config = json.load(f)
            config_for_a_list_of_the_object = list(
                filter(lambda x: x["object"] == object_name, config)
            )
            if len(config_for_a_list_of_the_object) == 1:
                return config_for_a_list_of_the_object[0]["config"][
                    "interval_time_to_extract"
                ]
            else:
                print(f"Object {object_name} is not found in the system configuration")

                raise
    except Exception as e:
        print(
            "An error occurs while getting data extraction configuration: ",
            traceback.format_exc(),
        )
        raise e
