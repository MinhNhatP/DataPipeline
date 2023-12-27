import datetime
import os
import traceback
from io import BytesIO as _BytesIO
from typing import Dict, Union, Any

import boto3
from dateutil.relativedelta import relativedelta

from .write_file import (
    write_parquet as _write_parquet,
    generate_table as _generate_table,
)
from .constant import PARTITION_TIME_CONNECTOR, DELTA_TIME_UNITS

S3_BUCKET = os.getenv("S3_BUCKET")
DATETIME_FORMAT = os.getenv("DATETIME_FORMAT")


def store_data_in_s3(event: Dict[str, Any]) -> None:
    try:
        # Values of partition_cols are from event
        partition_cols_values = {
            "object_name": event["object"],
            "from_date": event["time_window"]["from"],
            "to_date": event["time_window"]["to"],
        }
        # Get path of object
        path = _convert_time_range_to_partition_folder_path(**partition_cols_values)

        # Generate PyArrow Table from event's result
        table = _generate_table(event["result"], event["time_window"]["from"])

        if table is None:
            # If data is empty
            file = None
            obj_key = path

        else:
            # Else write to parquet format
            compression = "snappy"
            file, obj_key = _write_parquet(table, path, compression)

        # Upload to S3
        _upload_object_to_s3(file, obj_key)

    except Exception as e:
        print("Got error: ", traceback.format_exc())
        raise e


# TODO: Change folder structure to be more flexible
def _convert_time_range_to_partition_folder_path(
    object_name: str, from_date: str, to_date: str
) -> str:

    try:
        folder_path = f"object={object_name}/"
        from_date = datetime.datetime.strptime(from_date, DATETIME_FORMAT)
        to_date = datetime.datetime.strptime(to_date, DATETIME_FORMAT)

        for i, time_unit in enumerate(DELTA_TIME_UNITS):
            if getattr(from_date, time_unit) != getattr(to_date, time_unit):
                if _is_rounded_time_by_time_unit(to_date, time_unit):
                    if _is_rounded_time_by_time_unit(from_date, time_unit):
                        folder_path += (
                            f"{time_unit}={getattr(from_date, time_unit):02d}"
                            f"{PARTITION_TIME_CONNECTOR}"
                            f"{getattr(to_date, time_unit):02d}/"
                        )
                        break

                    if _is_delta_a_single_time_unit(to_date, from_date):
                        delta = relativedelta(to_date, from_date)
                        for j in range(i, len(DELTA_TIME_UNITS)):
                            time_unit_2 = DELTA_TIME_UNITS[j]
                            if getattr(delta, f"{time_unit_2}s") != 0:
                                folder_path += (
                                    f"{time_unit_2}="
                                    f"{getattr(from_date, time_unit_2):02d}"
                                    f"{PARTITION_TIME_CONNECTOR}"
                                    f"{getattr(to_date, time_unit_2):02d}/"
                                )
                                break
                            else:
                                folder_path += f"{time_unit_2}={getattr(from_date, time_unit_2):02d}/"
                        break

                    raise Exception(
                        "The current folder structure does support this time range"
                    )
                else:
                    # Found the last folder
                    folder_path += (
                        f"{time_unit}={getattr(from_date, time_unit):02d}"
                        f"{PARTITION_TIME_CONNECTOR}"
                        f"{getattr(to_date, time_unit):02d}/"
                    )
                    break
            else:
                folder_path += f"{time_unit}={getattr(from_date, time_unit):02d}/"

        return folder_path

    except Exception as e:
        print("Error when getting partition path of object: ", traceback.format_exc())
        raise e


def _is_delta_a_single_time_unit(to_date, from_date):
    delta = relativedelta(to_date, from_date)
    number_of_time_unit = 0
    for time_unit in DELTA_TIME_UNITS:
        if getattr(delta, f"{time_unit}s") != 0:
            number_of_time_unit += 1
    return number_of_time_unit == 1


def _is_rounded_time_by_time_unit(time: datetime, time_unit: str) -> bool:
    a_perfect_rounded_time = datetime.datetime(1000, 1, 1, 00, 00, 00)
    for i in reversed(DELTA_TIME_UNITS):
        if i != time_unit:
            if getattr(time, i) != getattr(a_perfect_rounded_time, i):
                return False
        else:
            break
    return True


def _upload_object_to_s3(file_object: Union[_BytesIO, None], obj_key: str) -> None:
    bucket = S3_BUCKET

    try:
        s3_resource = boto3.resource("s3")
        s3_client = s3_resource.meta.client

        if file_object is None:
            s3_client.put_object(Body=b"", Bucket=bucket, Key=obj_key)
        else:
            s3_client.upload_fileobj(Fileobj=file_object, Bucket=bucket, Key=obj_key)

    except Exception as e:
        print("Error when upload file to S3: ", traceback.format_exc())
        raise e
