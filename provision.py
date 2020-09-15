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
        # table_arn = self.create_table()

        self.attach_queue_event(queue_arn, lambda_arn)

    def kickoff(self):
        # try invoking:
        print(self.queue_push('{"foo": "bar"}'))

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

    # def create_table(self):
    #     def self.dynamo_client.create_table(
    #         TableName=table_name,
    #     )

    # def invoke_lambda(self):
    #     response = self.lambda_client.invoke(FunctionName=function_name)
    #     # print(response)
    #     return json.load(response["Payload"])


if __name__ == "__main__":
    if sys.argv[-1] == "provision":
        GrandIso().provision()
    if sys.argv[-1] == "reset":
        GrandIso().reset()
    if sys.argv[-1] == "kickoff":
        GrandIso().kickoff()