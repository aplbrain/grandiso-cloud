from typing import Optional
import sys
import json
import glob
import os
import io
import zipfile
from zipfile import ZipFile, ZipInfo
import time

import boto3
import tqdm

import networkx as nx

DEBUG = True

queue_name = "GrandIsoQ"
function_name = "GrandIsoLambda"

ASSUME_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "apigateway.amazonaws.com",
          "lambda.amazonaws.com",
          "events.amazonaws.com"
          "sqs.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""

FUNCTION_HANDLER = """

"""


class PermissiveZipFile(ZipFile):
    def writestr(self, zinfo_or_arcname, data, compress_type=None):
        if not isinstance(zinfo_or_arcname, ZipInfo):
            zinfo = ZipInfo(
                filename=zinfo_or_arcname, date_time=time.localtime(time.time())[:6]
            )

            zinfo.compress_type = self.compression
            if zinfo.filename[-1] == "/":
                zinfo.external_attr = 0o40775 << 16  # drwxrwxr-x
                zinfo.external_attr |= 0x10  # MS-DOS directory flag
            else:
                zinfo.external_attr = 0o664 << 16  # ?rw-rw-r--
        else:
            zinfo = zinfo_or_arcname

        super(PermissiveZipFile, self).writestr(zinfo, data, compress_type)


def _create_dynamo_table(
    table_name: str, primary_key: str, client, read_write_units: Optional[int] = None,
):
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    return client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": primary_key, "KeyType": "HASH"},  # Partition key
            # {"AttributeName": "title", "KeyType": "RANGE"},  # Sort key
        ],
        AttributeDefinitions=[
            {"AttributeName": primary_key, "AttributeType": "S"},
            # {"AttributeName": "title", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


def _dynamo_table_exists(table_name: str, client: boto3.client):
    """
    Check to see if the DynamoDB table already exists.

    Returns:
        bool: Whether table exists

    """
    existing_tables = client.list_tables()["TableNames"]
    return table_name in existing_tables


class GrandIso:
    def __init__(self):
        self.lambda_client = boto3.client(
            "lambda",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )
        self.iam_client = boto3.client(
            "iam",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )
        self.sqs_client = boto3.client(
            "sqs",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )
        self.dynamo_client = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        self.results_table_name = "GrandIsoResults"
        self.endpoint_url = "http://localhost:4566"

    def create_host(self):
        import grand
        from grand.backends import DynamoDBBackend

        G = grand.Graph(backend=DynamoDBBackend(dynamodb_url=self.endpoint_url))

        G.nx.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        G.nx.add_edge("C", "A")

    def _scan_table(self, table, scan_kwargs: dict = None):
        done = False
        start_key = None
        results = []
        scan_kwargs = scan_kwargs or {}
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = table.scan(**scan_kwargs)
            results += response.get("Items", [])
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return results

    def aggregate_results(self):
        dynamodb_resource = boto3.resource(
            "dynamodb",
            endpoint_url=self.endpoint_url,
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )
        results_table = dynamodb_resource.Table(self.results_table_name)
        return self._scan_table(results_table)

    def reset(self):
        # The grand iso is dead!
        self.purge_queue()
        self.teardown_lambda()

        # Long live the grand iso!
        self.provision()

    def provision(self):
        # provision resources

        queue_arn = self.create_queue()
        lambda_arn = self.create_lambda()
        table_arn = self.create_table()

        self.attach_queue_event(queue_arn, lambda_arn)

    def kickoff(self):
        # try invoking:
        motif = nx.DiGraph()
        motif.add_edge("A", "B")
        motif.add_edge("B", "C")
        motif.add_edge("C", "A")

        print(
            self.queue_push(
                json.dumps(
                    {
                        "motif": nx.readwrite.node_link_data(motif),
                        "candidate": {},
                        "ID": 1,
                    }
                )
            )
        )

    def purge_queue(self):
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        self.sqs_client.purge_queue(QueueUrl=queue_url)

    def teardown_lambda(self):
        self.lambda_client.delete_function(FunctionName=function_name)

    def attach_queue_event(self, queue_arn, lambda_arn):
        self.lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=lambda_arn,
            Enabled=True,
            BatchSize=1,
        )

    def queue_push(self, value):
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)["QueueUrl"]
        self.sqs_client.send_message(QueueUrl=queue_url, MessageBody=value)

    def create_queue(self):

        new_queue_request = self.sqs_client.create_queue(
            QueueName=queue_name, Attributes={"FifoQueue": "false"}
        )

        queue_arn = self.sqs_client.get_queue_attributes(
            QueueUrl=new_queue_request["QueueUrl"], AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        self.queue_arn = queue_arn

        return queue_arn

    def generate_zip(self):
        def zipdir(dpath, zipf):
            _debug_wrap = tqdm.tqdm if DEBUG else lambda x: x
            for fp in _debug_wrap(
                glob.glob(os.path.join(dpath, "**/*"), recursive=True)
            ):
                base = os.path.commonpath([dpath, fp])
                zipf.write(fp, arcname=fp.replace(base + "/", ""))

        mem_zip = io.BytesIO()

        vendor_dirs = ["lambda/vendor"]
        files = ["lambda/main.py"]

        with PermissiveZipFile(
            mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            for directory in vendor_dirs:
                zipdir(directory, zf)

            for file in files:
                zf.write(file, arcname=file.replace("lambda/", ""))

        return mem_zip.getvalue()

    def create_lambda(self):
        # First, create a lambda execution role in IAM.

        # This means creating a Policy for a user:
        try:
            role = self.iam_client.get_role(RoleName="grandiso_lambda_execution")
        except:
            role = self.iam_client.create_role(
                RoleName="grandiso_lambda_execution",
                AssumeRolePolicyDocument=ASSUME_POLICY,
            )
        role_arn = role["Role"]["Arn"]

        try:
            lambda_function = self.lambda_client.get_function(
                FunctionName=function_name
            )
            lambda_arn = lambda_function["Configuration"]["FunctionArn"]
        except:
            lambda_function = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime="python3.8",
                Role=role_arn,
                Handler="main.main",
                Code={"ZipFile": self.generate_zip()},
            )
            print(lambda_function)
            lambda_arn = lambda_function["FunctionArn"]
        return lambda_arn

    def create_table(self):
        if not _dynamo_table_exists(self.results_table_name, self.dynamo_client):
            _create_dynamo_table(self.results_table_name, "ID", self.dynamo_client)

    # def invoke_lambda(self):
    #     response = self.lambda_client.invoke(FunctionName=function_name)
    #     # print(response)
    #     return json.load(response["Payload"])


if __name__ == "__main__":
    if sys.argv[-1] == "provision":
        GrandIso().provision()
    if sys.argv[-1] == "reset":
        GrandIso().reset()
    if sys.argv[-1] == "create_host":
        GrandIso().create_host()
    if sys.argv[-1] == "kickoff":
        GrandIso().kickoff()
    if sys.argv[-1] == "aggregate_results":
        for res in GrandIso().aggregate_results():
            print(res)
