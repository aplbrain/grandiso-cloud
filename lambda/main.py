import json
import os

import boto3

from grand.backends import DynamoDBBackend

# LOCALSTACK_HOSTNAME = os.getenv("LOCALSTACK_HOSTNAME")
LOCALSTACK_HOSTNAME = "172.17.0.1"

ENDPOINT_URL = "http://" + LOCALSTACK_HOSTNAME + ":4566"


def main(event, lambda_context):

    DG = DynamoDBBackend(dynamodb_url=ENDPOINT_URL)

    print(DG.get_node_count())

    # sqs_client = boto3.client(
    #     "sqs",
    #     use_ssl=False,
    #     endpoint_url=ENDPOINT_URL,
    #     aws_access_key_id="foo",
    #     aws_secret_access_key="foo",
    #     region_name="us-east-1",
    # )

    # print("!!!")

    candidate = json.loads(event["Records"][0]["body"])
    # if candidate["foo"] == "bar":
    #     print("!!! foo is bar")
    #     queue_url = sqs_client.get_queue_url(QueueName="GrandIsoQ")["QueueUrl"]
    #     print("!!! got URL")
    #     sqs_client.send_message(QueueUrl=queue_url, MessageBody='{"foo": "baz"}')
    #     print("!!! sent message")
    #     result = json.dumps({"status": "queueing..."})
    #     print(result)
    #     return result

    result = json.dumps({"candidate": candidate})
    print(result)
    return result
