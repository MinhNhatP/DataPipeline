import json
import boto3
import traceback
import os

NR_CREDENTIALS_SECRET_ARN = os.getenv("SECRET_ARN")


def _retrieve_secret(secret_id: str) -> dict:
    try:
        session = boto3.session.Session()
        region_name = session.region_name
        client = session.client(service_name="secretsmanager", region_name=region_name)
        credentials = client.get_secret_value(SecretId=secret_id)
        if "SecretString" in credentials:
            credentials = credentials["SecretString"]
        credentials = json.loads(credentials)
        return credentials
    except Exception as e:
        exception_traceback = traceback.format_exc()
        print("Can not extract data with error: ", exception_traceback)
        raise e


def retrieve_credential():
    try:
        credentials = _retrieve_secret(secret_id=NR_CREDENTIALS_SECRET_ARN)
        user_key = credentials["token"]
        return user_key

    except Exception as e:
        exception_traceback = traceback.format_exc()
        print(
            "An error occurs while retrieving Snow credential from Secret Manager: ",
            exception_traceback,
        )
        raise e
