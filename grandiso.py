#!/usr/bin/env python3

# Type imports:
from typing import Optional, Tuple

# Standard library imports:
import argparse
import glob
import io
import json
import logging
import os
import sys
import time
import zipfile
from zipfile import ZipFile, ZipInfo

# Installed imports:
import boto3
import networkx as nx
import pandas as pd
import tqdm

# Note that for certain commands, you will also need to install Grand/GrandIso.

DEBUG = True

logging.basicConfig(level=logging.INFO)


queue_name_base = "GrandIsoQ"
function_name_base = "GrandIsoLambda"

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


class PermissiveZipFile(ZipFile):
    """
    A Zipfile class that also sets file permission flags.

    This is required for the lambda runtime. Thanks to the answerers at
    https://stackoverflow.com/questions/434641/ for insights and useful code.

    """

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


def _scan_table(table, scan_kwargs: dict = None):
    """
    DynamoDB convenience function to scan all results from a table.
    """
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


def _create_dynamo_table(
    table_name: str,
    primary_key: str,
    client: boto3.client,
    read_write_units: Optional[int] = None,
):
    """
    DynamoDB convenience function to create a new DynamoDB table.

    """
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    return client.create_table(
        TableName=table_name,
        KeySchema=[
            # Partition/hash key
            {"AttributeName": primary_key, "KeyType": "HASH"},
        ],
        AttributeDefinitions=[{"AttributeName": primary_key, "AttributeType": "S"},],
        # Currently do not support billing methods besides on-demand.
        BillingMode="PAY_PER_REQUEST",
    )


def _dynamo_table_exists(table_name: str, client: boto3.client):
    """
    DynamoDB convenience function to check if a DynamoDB table already exists.

    Returns:
        bool: Whether table exists

    """
    existing_tables = client.list_tables()["TableNames"]
    return table_name in existing_tables


class GrandIso:
    """
    A class responsible for creating, managing, and manipulating GrandIso jobs
    in the cloud.

    Certain resources, like the Lambda and SQS queue, are reused across all of
    the GrandIso jobs that you run on the same account. Other resources, like
    the results table in DynamoDB, are specific to individual jobs.

    """

    def __init__(
        self,
        endpoint_url: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        dry: bool = False,
    ):
        """
        Create a new GrandIso client.

        Arguments:
            dry (bool: False): Whether to try-run (True) or actually perform
                changes (False). Defaults to False (i.e. will actually kick
                off a job when you ask it to.)
            endpoint_url (str: None): The URL against which to run commands
                (e.g. an AWS URL or a localstack URL). If none is provided, the
                default is AWS. You can also provide `"http://localhost:4566"`
                to run against an already-running localstack.
            aws_access_key_id (str: None): Optional credentials for AWS
            aws_secret_access_key (str: None): Optional credentials for AWS

        """
        _aws_kwargs = {}
        if aws_access_key_id:
            _aws_kwargs["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            _aws_kwargs["aws_secret_access_key"] = aws_secret_access_key
        if endpoint_url:
            _aws_kwargs["endpoint_url"] = endpoint_url

        self.dry = dry
        self.lambda_client = boto3.client("lambda", **_aws_kwargs,)
        self.iam_client = boto3.client("iam", **_aws_kwargs,)
        self.sqs_client = boto3.client("sqs", **_aws_kwargs,)
        self.dynamo_client = boto3.client("dynamodb", **_aws_kwargs,)

        self.results_table_name = "GrandIsoResults"
        self.endpoint_url = endpoint_url
        self._lambda_execution_role_name = "GrandIsoLambdaExecutionRole"

        self.log = logging

    ##
    #
    # Teardown
    #
    ##

    def _teardown_lambda(self):
        return self.lambda_client.delete_function(FunctionName=function_name_base)

    def _teardown_queue(self):
        return

    def _teardown_tables(self):
        return

    def teardown(self):
        """
        Tear down all resources associated with GrandIso in this account.

        Note that this can take several minutes, and specifically when deleting
        a SQS Queue, you may not be able to create a new Queue with the same
        name until several minutes have passed.

        If you are trying to start fresh, you should NOT call the GrandIso
        teardown function, and instead you should remove the resources specific
        to your job.

        """
        self._teardown_lambda()
        self._teardown_queue()
        self._teardown_tables()
        return

    def cli_teardown(self, argparser_args=None):
        """
        Run the teardown command from the command-line.

        See GrandIso#teardown.

        """
        self.log.debug("Starting the teardown process.")
        if self.dry:
            self.log.info("This will tear down the following resources:")
            self.log.info(
                f" - (IAM) Lambda Execution Role: {self._lambda_execution_role_name}"
            )
            self.log.info(f" - (Lambda) Lambda Function:    {function_name_base}")
            self.log.info(f" - (SQS) Queue:                 {queue_name_base}")
            self.log.info(f" - (DynamoDB) Table:            {self.results_table_name}")
            return
        # Perform the actual teardown:
        self.teardown()
        self.log.debug("Completed teardown process.")

    ##
    #
    # Purging & Cancellation
    #
    ##

    def _purge_queue(self):
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name_base)["QueueUrl"]
        self.sqs_client.purge_queue(QueueUrl=queue_url)

    def cancel(self):
        self._purge_queue()
        return

    def create_host(self):
        import grand
        from grand.backends import DynamoDBBackend

        G = grand.Graph(backend=DynamoDBBackend(dynamodb_url=self.endpoint_url))

        G.nx.add_edge("A", "B")
        G.nx.add_edge("B", "C")
        G.nx.add_edge("C", "A")

    ##
    #
    # Results
    #
    ##

    def aggregate_results(self):
        dynamodb_resource = boto3.resource(
            "dynamodb",
            endpoint_url=self.endpoint_url,
            aws_access_key_id="foo",
            aws_secret_access_key="foo",
        )
        results_table = dynamodb_resource.Table(self.results_table_name)
        return _scan_table(results_table)

    def cli_results(self, argparser_args=None):
        if argparser_args.format == "csv":
            return pd.DataFrame(
                [res["candidate"] for res in self.aggregate_results()]
            ).to_csv()
        if argparser_args.format == "json":
            return pd.DataFrame(
                [res["candidate"] for res in self.aggregate_results()]
            ).to_json()
        if argparser_args.format == "raw":
            return self.aggregate_results()

    def print_cli_results(self, argparser_args=None):
        print(self.cli_results(argparser_args))

    ##
    #
    # Provisioning
    #
    ##

    def create_queue(self):
        new_queue_request = self.sqs_client.create_queue(
            QueueName=queue_name_base, Attributes={"FifoQueue": "false"}
        )

        queue_arn = self.sqs_client.get_queue_attributes(
            QueueUrl=new_queue_request["QueueUrl"], AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        self.queue_arn = queue_arn

        return queue_arn

    def create_lambda(self):
        """
        Create a lambda for GrandIso in general.

        Note that this is not specific to a job, and so it is not prefixed with
        namespacing variables.

        """
        # First, create a lambda execution role in IAM.
        # This means creating a Policy for a user:
        try:
            role = self.iam_client.get_role(RoleName=self._lambda_execution_role_name)
        except:
            role = self.iam_client.create_role(
                RoleName=self._lambda_execution_role_name,
                AssumeRolePolicyDocument=ASSUME_POLICY,
            )
        role_arn = role["Role"]["Arn"]

        # Next, create the lambda itself:
        try:
            lambda_function = self.lambda_client.get_function(
                FunctionName=function_name_base
            )
            lambda_arn = lambda_function["Configuration"]["FunctionArn"]
        except:
            lambda_function = self.lambda_client.create_function(
                FunctionName=function_name_base,
                Runtime="python3.8",
                Role=role_arn,
                Handler="main.main",
                Code={"ZipFile": self.generate_zip()},
            )
            # print(lambda_function)
            lambda_arn = lambda_function["FunctionArn"]
        return lambda_arn

    def create_table(self):
        """
        Create a DynamoDB table to hold the results from this job.

        """
        if not _dynamo_table_exists(self.results_table_name, self.dynamo_client):
            return _create_dynamo_table(
                self.results_table_name, "ID", self.dynamo_client
            )
        return False

    def attach_queue_event(self, queue_arn, lambda_arn):
        self.lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=lambda_arn,
            Enabled=True,
            BatchSize=1,
        )

    def provision(self) -> Tuple[str, str, str]:
        queue_arn = self.create_queue()
        lambda_arn = self.create_lambda()
        table_arn = self.create_table()
        return (queue_arn, lambda_arn, table_arn)

    def cli_provision(self, argparser_args=None):
        self.log.debug("Starting the resource provisioning process.")
        if self.dry:
            self.log.info("This will provision the following resources:")
            self.log.info(
                f" - (IAM) Lambda Execution Role: {self._lambda_execution_role_name}"
            )
            self.log.info(f" - (Lambda) Lambda Function:    {function_name_base}")
            self.log.info(f" - (SQS) Queue:                 {queue_name_base}")
            self.log.info(f" - (DynamoDB) Table:            {self.results_table_name}")
            return

        (queue_arn, lambda_arn, table_arn) = self.provision()

        self.log.debug(f"Created table with ARN [{table_arn}].")

        self.log.debug(f"Attaching lambda [{lambda_arn}] to queue [{queue_arn}].")
        self.attach_queue_event(queue_arn, lambda_arn)
        self.log.debug("Completed resource provisioning process.")

    ##
    #
    # Kickoff
    #
    ##

    def queue_push(self, value):
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name_base)["QueueUrl"]
        self.sqs_client.send_message(QueueUrl=queue_url, MessageBody=value)

    def kickoff(self, motif_nx: nx.Graph, initial_candidate: dict = None):

        initial_queue_item = json.dumps(
            {
                # A version of this motif:
                "motif": nx.readwrite.node_link_data(motif_nx),
                # An empty candidate (formerly called "backbone"):
                "candidate": initial_candidate or {},
                # An arbitrary ID as PK:
                # TODO: uuid4
                "ID": 1,
                # TODO: Add job name
            }
        )
        # TODO: Do something cleverer than printing this:
        self.log.debug(self.queue_push(initial_queue_item))

    def cli_kickoff(self, argparser_args=None):
        if self.dry:
            self.log.info("This will begin the motif search in the graph.")
            return

        # TODO: construct the motif from disk
        motif = nx.DiGraph()
        motif.add_edge("A", "B")
        motif.add_edge("B", "C")
        motif.add_edge("C", "A")

        self.kickoff(motif)

    def generate_zip(self):
        """
        Construct a zip file from the vendored and nonvendored libraries.

        """
        # TODO: Move to module top level

        def zipdir(dpath, zipf):
            """
            Zip an entire directory, recursively.
            """
            _debug_wrap = tqdm.tqdm if DEBUG else lambda x: x
            for fp in _debug_wrap(
                glob.glob(os.path.join(dpath, "**/*"), recursive=True)
            ):
                base = os.path.commonpath([dpath, fp])
                zipf.write(fp, arcname=fp.replace(base + "/", ""))

        mem_zip = io.BytesIO()

        # A list of vendor directories:
        vendor_dirs = ["lambda/vendor"]

        # A list of files to include at root level:
        files = ["lambda/main.py"]

        with PermissiveZipFile(
            mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            for directory in vendor_dirs:
                zipdir(directory, zf)

            for file in files:
                zf.write(file, arcname=file.replace("lambda/", ""))

        return mem_zip.getvalue()


def cli_main():
    parser = argparse.ArgumentParser(
        description="GrandIso subgraph isomorphism in the cloud"
    )

    # Global arguments:

    parser.add_argument(
        "--job-name",
        type=str,
        required=False,
        help=(
            "The job name to use to refer to this subgraph isomorphism "
            + "search. Make one up when calling `provision`, and then reuse "
            + "the same name for the other commands."
        ),
    )
    parser.add_argument(
        "--dry",
        type=bool,
        required=False,
        default=False,
        help="Whether to dry-run (instead of actually running)",
    )

    grandiso = GrandIso(
        endpoint_url="http://localhost:4566",
        aws_access_key_id="grandiso",
        aws_secret_access_key="grandiso",
        dry=True,
    )

    subparsers = parser.add_subparsers(help="Commands.")

    # Provision
    provision_command = subparsers.add_parser(
        "provision", help="Provision resources for GrandIso."
    )
    provision_command.set_defaults(func=grandiso.cli_provision)

    # Teardown
    teardown_command = subparsers.add_parser(
        "teardown", help="Remove all traces of GrandIso from this AWS account."
    )
    teardown_command.set_defaults(func=grandiso.cli_teardown)

    # Kickoff
    kickoff_command = subparsers.add_parser(
        "kickoff", help="Start the search for a given motif."
    )
    kickoff_command.add_argument(
        "--motif",
        type=str,
        required=True,
        help=(
            "The motif for which to begin searching. Should be a file that "
            + "contains a .motif definition in the DotMotif DSL. Note that "
            + "only structural (edges and nodes) and attribute information "
            + "(a.weight == 5) will be searched; attribute comparator "
            + "information (a.weight > 5) will be omitted.",
        ),
    )
    kickoff_command.set_defaults(func=grandiso.cli_kickoff)

    # Kickoff
    results_command = subparsers.add_parser(
        "results", help="Print the results from the search."
    )
    results_command.add_argument(
        "--format",
        default="csv",
        type=str,
        required=False,
        help="The format (csv|json) in which to return results.",
    )
    results_command.set_defaults(func=grandiso.print_cli_results)

    # Currently does not support `reset`. I think that's correct...

    # Parse args:
    args = parser.parse_args()
    grandiso.dry = args.dry
    args.func(args)

    # if sys.argv[-1] == "create_host":
    #     GrandIso().create_host()
    # if sys.argv[-1] == "aggregate_results":
    #     for res in GrandIso().aggregate_results():
    #         print(res)


if __name__ == "__main__":

    cli_main()
