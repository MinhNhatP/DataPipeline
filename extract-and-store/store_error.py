import datetime
import json
import os
import traceback
import uuid
import boto3
from util.store_data import _convert_time_range_to_partition_folder_path

EXTRACTION_ERROR_DB_TABLE = os.getenv("EXTRACTION_ERROR_DB_TABLE")
DATETIME_FORMAT = os.getenv("DATETIME_FORMAT")
S3_BUCKET = os.getenv("S3_BUCKET")

dynamo = boto3.client("dynamodb")
s3 = boto3.resource("s3")


def _convert_event_to_dynamodb_item(event):
    error_type = event["error"]["Error"]
    cause = event["error"]["Cause"]
    try:
        error = json.loads(cause)
        error_message = error["errorMessage"]
        error_stack_trace = (
            error["stackTrace"] if len(error["stackTrace"]) > 0 else [cause]
        )
    except (ValueError, KeyError) as e:
        error_message = error_type
        error_stack_trace = [cause]

    item = {
        "key": {"S": str(uuid.uuid4())},
        "object": {"S": event["object"]},
        "time_window_from": {"S": event["time_window"]["from"]},
        "time_window_to": {"S": event["time_window"]["to"]},
        "error_type": {"S": error_type},
        "error_message": {"S": error_message},
        "error_stack_trace": {"SS": error_stack_trace},
        "created_date": {"S": datetime.datetime.now().strftime(DATETIME_FORMAT)},
    }
    if "parent" in event:
        item["parent"] = {"S": event["parent"]}

    return item


def _store_error_in_dynamodb(event):
    try:
        dynamodb_item = _convert_event_to_dynamodb_item(event)
        dynamo.put_item(TableName=EXTRACTION_ERROR_DB_TABLE, Item=dynamodb_item)

        return dynamodb_item

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("An error occurs while putting errors into dynamo: ", exception_traceback)


def _delete_files(event):
    bucket = s3.Bucket(S3_BUCKET)
    object_path = _convert_time_range_to_partition_folder_path(
        event["object"], event["time_window"]["from"], event["time_window"]["to"]
    )
    bucket.objects.filter(Prefix=object_path).delete()


def lambda_handler(event, context):
    try:
        _store_error_in_dynamodb(event)
        # _delete_files(event)

    except Exception as e:
        raise e
