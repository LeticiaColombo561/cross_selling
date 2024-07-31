import json
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import botocore.config

from cs.utils import access_secret_version
from cs.config import SECRET_CONFIG

logging.basicConfig(level="INFO")


class SNSDataSave:
    def __init__(
            self,
            access_key=None,
            secret_key=None,
            region="us-east-2",
            topic=None,
            max_pool=200,
    ) -> None:
        self.region = region
        self.client = None
        self.max_pool = max_pool
        self.topic = topic

        if access_key and secret_key:
            self._set_client(access_key, secret_key, region)

    def _set_client(
            self,
            access_key=None,
            secret_key=None,
            region="us-east-2",
    ):
        client_config = botocore.config.Config(
            max_pool_connections=self.max_pool,
        )
        self.client = boto3.client(
            "sns",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=client_config,
        )

    def set_client_from_gcp(self,
                            credential=None,
                            env="stg"
                            ):
        project_id = SECRET_CONFIG[env]["project"]
        secret_id = SECRET_CONFIG[env]["secret_id"]
        aws_cred = json.loads(access_secret_version(project_id, secret_id, credentials=credential))
        access_key = aws_cred["AWS_ACCESS_KEY_ID"]
        secret_key = aws_cred["AWS_SECRET_ACCESS_KEY"]
        self._set_client(access_key, secret_key, self.region)

    def publish(self,
                message,
                attr_message,
                topic_name=None
                ):
        if not self.client:
            self._set_client()

        topic = topic_name if topic_name else self.topic

        response = self.client.publish(
            TargetArn=topic,
            Message=json.dumps({"default": json.dumps(message)}),
            MessageStructure="json",
            MessageAttributes={
                'event_type': {
                    'DataType': 'String',
                    'StringValue': str(attr_message)
                }
            }

        )
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code != 200:
            raise ConnectionError(status_code)
        return response

    def publish_many(self,
                     messages,
                     attr_message,
                     topic_name=None
                     ):
        topic = topic_name if topic_name else self.topic

        results = []
        with ThreadPoolExecutor(max_workers=self.max_pool) as executor:
            for msg in messages:
                result = executor.submit(self.publish, msg, attr_message, topic)
                results.append(result)
            status = [r.result() for r in results]
            logging.debug(status[:10])
            logging.info(f"{len(status)} messages were send to SNS")
