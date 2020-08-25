import json
import boto3
import io
import zipfile
from zipfile import ZipFile, ZipInfo
import time

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

ATTACH_POLICY = """{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "logs:*"
            ],
            "Resource": "arn:aws:logs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "xray:PutTraceSegments",
                "xray:PutTelemetryRecords"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AttachNetworkInterface",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeInstances",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DetachNetworkInterface",
                "ec2:ModifyNetworkInterfaceAttribute",
                "ec2:ResetNetworkInterfaceAttribute"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:*"
            ],
            "Resource": "arn:aws:kinesis:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sns:*"
            ],
            "Resource": "arn:aws:sns:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "sqs:*"
            ],
            "Resource": "arn:aws:sqs:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "dynamodb:*"
            ],
            "Resource": "arn:aws:dynamodb:*:*:*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "route53:*"
            ],
            "Resource": "*"
        }
    ]
}"""

FUNCTION_HANDLER = """
import json

def main(event, lambda_context):
    return json.dumps({"working": True, "event": event})

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
        pass

    def teardown(self):
        self.teardown_queue()
        self.teardown_lambda()

    def provision(self):
        # provision resources

        queue_arn = self.create_queue()
        lambda_arn = self.create_lambda()

        # try invoking:
        print(self.invoke_lambda())

    def refresh(self):
        self.teardown()
        self.provision()

    def teardown_queue(self):
        pass

    def teardown_lambda(self):
        lambda_client = boto3.client(
            "lambda",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        lambda_client.delete_function(FunctionName=function_name)

    def create_queue(self):
        sqs_client = boto3.client(
            "sqs",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        new_queue_request = sqs_client.create_queue(
            QueueName=queue_name, Attributes={"FifoQueue": "false"}
        )

        queue_arn = sqs_client.get_queue_attributes(
            QueueUrl=new_queue_request["QueueUrl"], AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]

        return queue_arn

    def generate_zip(self):
        mem_zip = io.BytesIO()

        files = [("grandiso.py", FUNCTION_HANDLER)]

        with PermissiveZipFile(
            mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            for name, bytes in files:
                zf.writestr(name, bytes)

        return mem_zip.getvalue()

    def create_lambda(self):
        # First, create a lambda execution role in IAM.

        iam_client = boto3.client(
            "iam",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        try:
            role = iam_client.get_role(RoleName="grandiso_lambda_execution")
        except:
            role = iam_client.create_role(
                RoleName="grandiso_lambda_execution",
                AssumeRolePolicyDocument=ASSUME_POLICY,
            )
        role_arn = role["Role"]["Arn"]

        lambda_client = boto3.client(
            "lambda",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        try:
            lambda_function = lambda_client.get_function(FunctionName=function_name)
            lambda_arn = lambda_function["Configuration"]["FunctionArn"]
        except:
            lambda_function = lambda_client.create_function(
                FunctionName=function_name,
                Runtime="python3.8",
                Role=role_arn,
                Handler="grandiso.main",
                Code={"ZipFile": self.generate_zip()},
            )
            print(lambda_function)
            lambda_arn = lambda_function["FunctionArn"]
        return lambda_arn

    def invoke_lambda(self):
        lambda_client = boto3.client(
            "lambda",
            endpoint_url="http://localhost:4566",
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )

        response = lambda_client.invoke(FunctionName=function_name)
        # print(response)
        return json.load(response["Payload"])


if __name__ == "__main__":
    GrandIso().refresh()
