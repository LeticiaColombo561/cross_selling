from cs.config import AWS_CONFIG
from google.oauth2 import service_account
from cs.sns_data_save import SNSDataSave
import google
import json
import numpy as np
import decimal


def get_gcp_credentials(service_account_info):
    if isinstance(service_account_info, dict):
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        project_id = credentials.project_id
    elif isinstance(service_account_info, str) and service_account_info:
        service_account_info = json.loads(service_account_info)
        credentials = service_account.Credentials.from_service_account_info(
            service_account_info
        )
        project_id = credentials.project_id
    else:
        credentials, project_id = google.auth.default()

    return credentials, project_id


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def export_to_sns(data_set, attr_message, service_account_info=None, env="stg"):
    try:
        env = env.lower()
    except:
        print("An exception occurred with:", env)

    config = AWS_CONFIG[env]

    credentials, project_id = get_gcp_credentials(service_account_info)

    client = SNSDataSave(
        region=config["region"],
        topic=config["topic"],
    )
    # TODO: get the credencial here and pass as argument of constructor
    client.set_client_from_gcp(credentials, env)
    client.publish_many(data_set, attr_message)

    print("Aws config:", config)
