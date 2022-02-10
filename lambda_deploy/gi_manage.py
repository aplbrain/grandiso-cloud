#!/usr/bin/env python3

# Type imports:
from typing import Callable, Iterable, Optional, Tuple, Union

# Standard library imports:
import argparse
import glob
import io
import json
import logging
import os
import time
import zipfile
from zipfile import ZipFile, ZipInfo

# Installed imports:
import boto3
from boto3.dynamodb.conditions import Key
import networkx as nx
import pandas as pd
import tqdm

# Note that for certain commands, you will also need to install Grand/GrandIso.

# Global configuration parameters:

# Whether to perform extra-verbose outputs. Useful for debugging during the
# development of this library, not super important for everyday usage.
DEBUG = True

logging.basicConfig(level=logging.INFO)

# The prefix for AWS resources. This is helpful to keep track of all GI-Cloud
# resources if Teardown commands fail.
queue_name_base = "GrandIsoQ"
function_name_base = "GrandIsoLambda"

# The IAM role for the Lambda to assume. A more restrictive version than is
# commonly used for things like Zappa.
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
            # Save-time is Tuple[int]: (YYYY, M, D, H, m, s)
            date_time = time.localtime(time.time())[:6]
            zinfo = ZipInfo(filename=zinfo_or_arcname, date_time=date_time)

            zinfo.compress_type = self.compression
            if zinfo.filename[-1] == "/":
                zinfo.external_attr = 0o40775 << 16  # drwxrwxr-x
                zinfo.external_attr |= 0x10  # MS-DOS directory flag
            else:
                zinfo.external_attr = 0o664 << 16  # ?rw-rw-r--
        else:
            zinfo = zinfo_or_arcname

        super(PermissiveZipFile, self).writestr(zinfo, data, compress_type)


def _generate_zip(vendor_directories: list = None, include_files: list = None):
    """
    Construct a zip file from the vendored and nonvendored libraries.

    """

    def zipdir(dpath, zipf):
        """
        Zip an entire directory, recursively.
        """
        _debug_wrap = tqdm.tqdm if DEBUG else lambda x: x
        for fp in _debug_wrap(
            glob.glob(os.path.join(dpath, "**/*"), recursive=True),
        ):
            base = os.path.commonpath([dpath, fp])
            zipf.write(fp, arcname=fp.replace(base + "/", ""))

    mem_zip = io.BytesIO()

    # A list of vendor directories:
    vendor_dirs = vendor_directories or ["lambda/vendor"]

    # A list of files to include at root level:
    files = include_files or ["lambda/main.py"]

    with PermissiveZipFile(
        mem_zip,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
    ) as zf:
        for directory in vendor_dirs:
            zipdir(directory, zf)

        for file in files:
            zf.write(file, arcname=file.replace("lambda/", ""))

    return mem_zip.getvalue()


class Motif:
    """
    A class to manage reading motifs in a variety of formats.
    """

    def __init__(self, graph: nx.Graph) -> None:
        """
        Create a new Motif from a file, graph, or other structure.

        Arguments:
            graph (nx.Graph): A source graph to turn into a motif (using the
                DotMotif library)

        """
        self.graph = graph

    @staticmethod
    def from_file(filename: str, directed: bool = True) -> "Motif":
        """
        Create a new Motif object from a .motif file or .csv file.

        Arguments:
            filename (str): The file path to read
            directed (bool = True): If the file details a directed (True) or
                undirected (False) motif

        Returns:
            Motif

        """
        if filename.endswith(".motif"):
            return Motif.from_motif(filename, directed)
        return Motif.from_edgelist(filename, directed)

    @staticmethod
    def from_motif(filename: str, directed: bool = True) -> "Motif":
        """
        Create a new Motif object from a .motif file.

        Arguments:
            filename (str): The file path to read
            directed (bool = True): If the file details a directed (True) or
                undirected (False) motif

        Returns:
            Motif

        """
        raise NotImplementedError()
        # return Motif()

    @staticmethod
    def from_edgelist(filename: str, directed: bool = True) -> "Motif":
        """
        Render a .csv file to a networkx.Graph.

        Arguments:
            filename (str): The file path to read
            directed (bool = True): If the file details a directed (True) or
                undirected (False) motif

        Returns:
            Motif

        """
        return Motif(
            graph=nx.read_edgelist(
                filename, create_using=(nx.DiGraph if directed else nx.Graph)
            )
        )

    def to_nx(self) -> nx.Graph:
        """
        Export the Motif object to its underlying networkx.Graph.

        Arguments:
            None

        Returns:
            nx.Graph: The graph representation of the motif.

        """
        return self.graph


def _scan_table(table: "Table", scan_kwargs: dict = None):
    """
    DynamoDB convenience function to scan all results from a table.

    Arguments:
        table (boto3.Resource): The boto3 DynamoDB Table object to use for
            database operations
        scan_kwargs (dict = None): Arbitrary additional keyword arguments to
            pass to the Table#scan operation

    Returns:
        List

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
    Convenience function to create a new DynamoDB table.

    Arguments:
        table_name (str): The name of the table to create
        primary_key (str): The name of the primary key for the new table. This
            is the DynamoDB "hash" key.
        client (boto3.client): The boto3 DynamoDB client
        read_write_units (int = None): The read/write units to provision for
            the new table. If set to None, the default behavior to to charge
            per-request.

    Returns:
        boto3.Resource: The boto3 DynamoDB table object

    """
    if read_write_units is not None:
        raise NotImplementedError("Non-on-demand billing is not currently supported.")

    return client.create_table(
        TableName=table_name,
        KeySchema=[
            # Partition/hash key
            {"AttributeName": primary_key, "KeyType": "HASH"},
        ],
        AttributeDefinitions=[
            {"AttributeName": primary_key, "AttributeType": "S"},
        ],
        # Currently do not support billing methods besides on-demand.
        BillingMode="PAY_PER_REQUEST",
    )


def _dynamo_table_exists(table_name: str, client: boto3.client):
    """
    DynamoDB convenience function to check if a DynamoDB table already exists.

    Arguments:
        table_name (str): The name of the table to check
        client (boto3.client): The boto3 DynamoDB client

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
        self.lambda_client = boto3.client("lambda", **_aws_kwargs)
        self.iam_client = boto3.client("iam", **_aws_kwargs)
        self.sqs_client = boto3.client("sqs", **_aws_kwargs)
        self.dynamo_client = boto3.client("dynamodb", **_aws_kwargs)
        self.dynamodb_resource = boto3.resource("dynamodb", **_aws_kwargs)

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
        self._purge_queue()
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

        Arguments:
            None

        Returns:
            None

        """
        self._teardown_queue()
        self._teardown_lambda()
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
        """
        Cancel all GrandIso jobs in this account.

        This will cancel the jobs but not delete the AWS resources.

        Arguments:
            None

        Returns:
            None

        """
        self._purge_queue()
        return

    ##
    #
    # Results
    #
    ##

    def aggregate_results(self, job: str) -> Iterable:
        """
        Get the results of a GrandIso job.

        Arguments:
            job (str): The job to get results for

        Returns:
            Iterable: The results of the job

        """

        results_table = self.dynamodb_resource.Table(self.results_table_name)
        return _scan_table(results_table, {"FilterExpression": Key("job").eq(job)})

    def cli_results(self, argparser_args=None):
        """
        Run the results command from the command-line.

        See GrandIso#aggregate_results.

        """
        if not argparser_args.job:
            raise ValueError("Job must be specified.")
        job_name = argparser_args.job
        if argparser_args.format == "csv":
            return pd.DataFrame(
                [res["candidate"] for res in self.aggregate_results(job_name)]
            ).to_csv()
        if argparser_args.format == "json":
            return pd.DataFrame(
                [res["candidate"] for res in self.aggregate_results(job_name)]
            ).T.to_json()
        if argparser_args.format == "raw":
            return self.aggregate_results(job_name)

    def print_cli_results(self, argparser_args=None):
        """
        Run the results command from the command-line, and print the results.

        See GrandIso#cli_results.

        """
        print(self.cli_results(argparser_args))

    ##
    #
    # Provisioning
    #
    ##

    def create_queue(self) -> str:
        """
        Create a new SQS Queue.

        Arguments:
            None

        Returns:
            str: The ARN URI of the new queue

        """
        new_queue_request = self.sqs_client.create_queue(
            QueueName=queue_name_base, Attributes={"FifoQueue": "false"}
        )

        queue_arn = self.sqs_client.get_queue_attributes(
            QueueUrl=new_queue_request["QueueUrl"], AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        self.queue_arn = queue_arn

        return queue_arn

    def create_lambda(self) -> str:
        """
        Create a lambda for GrandIso in general.

        Note that this is not specific to a job, and so it is not prefixed with
        namespacing variables.

        Arguments:
            None

        Returns:
            str: The ARN URI of the new lambda

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
                # Note that you can optionally pass `vendor_directory` and
                # `include_files` arguments to _generate_zip in order to curate
                # which files are included in the zipfile.
                Code={"ZipFile": _generate_zip()},
            )
            lambda_arn = lambda_function["FunctionArn"]
        return lambda_arn

    def create_table(self) -> Union[str, bool]:
        """
        Create a DynamoDB table to hold the results from this job.

        Arguments:
            None

        Returns:
            str: The ARN URI of the new table, or False if the table already
                exists and does not need to be created.

        """
        if not _dynamo_table_exists(self.results_table_name, self.dynamo_client):
            return _create_dynamo_table(
                self.results_table_name, "ID", self.dynamo_client
            )
        return False

    def attach_queue_event(self, queue_arn: str, lambda_arn: str, batch_size: int = 1):
        """
        Attach the SQS new-item event handler to the lambda.

        Arguments:
            queue_arn (str): The ARN of the SQS queue
            lambda_arn (str): The ARN of the Lambda function

        Returns:
            None

        """
        self.lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=lambda_arn,
            Enabled=True,
            BatchSize=batch_size,
        )

    def provision(self) -> Tuple[str, str, str]:
        """
        Create the queue, lambda, and table for Grand Iso cloud jobs.

        Arguments:
            None

        Returns:
            Tuple[str, str, str]: The ARN of the queue, the ARN of the lambda,
                and the ARN of the table (in that order).

        """
        queue_arn = self.create_queue()
        lambda_arn = self.create_lambda()
        table_arn = self.create_table()
        return (queue_arn, lambda_arn, table_arn)

    def cli_provision(self, argparser_args=None):
        """
        Run the provision command from the command-line.

        See GrandIso#provision.

        """
        batch_size = int(argparser_args.get("batch_size", 1))
        self.log.debug("Starting the resource provisioning process.")
        if self.dry:
            self.log.info("This will provision the following resources:")
            self.log.info(
                f" - (IAM) Lambda Execution Role: {self._lambda_execution_role_name}"
            )
            self.log.info(f" - (Lambda) Lambda Function:    {function_name_base}")
            self.log.info(f" - (SQS) Queue:                 {queue_name_base}")
            self.log.info(f" - (DynamoDB) Table:            {self.results_table_name}")
            self.log.info(f" - SQS Event Trigger Batch:     {batch_size}")
            return

        (queue_arn, lambda_arn, table_arn) = self.provision()

        self.log.debug(f"Created table with ARN [{table_arn}].")

        self.log.debug(f"Attaching lambda [{lambda_arn}] to queue [{queue_arn}].")
        self.attach_queue_event(queue_arn, lambda_arn, batch_size=batch_size)
        self.log.debug("Completed resource provisioning process.")

    ##
    #
    # Kickoff
    #
    ##

    def queue_push(self, value: str):
        """
        Push a new item to the queue.

        Arguments:
            value (str): The value to push to the queue.

        Returns:
            None

        """
        queue_url = self.sqs_client.get_queue_url(QueueName=queue_name_base)["QueueUrl"]
        self.sqs_client.send_message(QueueUrl=queue_url, MessageBody=value)

    def kickoff(
        self,
        motif_nx: nx.Graph,
        job_name: str,
        directed: bool = True,
        initial_candidate: dict = None,
    ):
        """
        Kick off a new job.

        Arguments:
            motif_nx (nx.Graph): The motif to search for.
            job_name (str): The name of the job.
            directed (bool): Whether the motif is directed.
            initial_candidate (dict): The initial candidate to start the search

        Returns:
            None

        """
        initial_queue_item = json.dumps(
            {
                # A version of this motif:
                "motif": nx.readwrite.node_link_data(motif_nx),
                # An empty candidate (formerly called "backbone"):
                "candidate": initial_candidate or {},
                # An arbitrary ID as PK:
                # "ID": str(uuid4()),
                # Add job name
                "job": job_name,
                "directed": directed,
            }
        )
        # TODO: Do something cleverer than printing this:
        self.log.debug(self.queue_push(initial_queue_item))

    def cli_kickoff(self, argparser_args=None):
        """
        Kick off a new job from the command-line.

        See GrandIso#kickoff.

        """
        if not argparser_args.job:
            raise ValueError(
                "You must provide a job name. From the command-line, you can specify one with --job."
            )
        job_name = argparser_args.job
        directed = argparser_args.directed
        if not argparser_args.motif:
            raise ValueError(
                "You must provide a motif. From the command-line, you can specify a file that contains a motif with --motif."
            )
        # TODO: Directed vs non...from cli
        motif = Motif.from_file(argparser_args.motif, directed=directed).to_nx()

        if self.dry:
            self.log.info("This will begin the motif search in the graph.")
            self.log.info("Motif statistics:")
            self.log.info(f"  |V|: {len(motif)}")
            self.log.info(f"  |E|: {len(motif.edges())}")
            return

        self.kickoff(motif, job_name, directed=directed)


def cli_main():
    """
    Run the command-line interface.

    Arguments:
        None

    Returns:
        None

    """
    parser = argparse.ArgumentParser(
        description="GrandIso subgraph isomorphism in the cloud"
    )

    # Global arguments:

    parser.add_argument(
        "--dry",
        type=bool,
        required=False,
        default=False,
        help="Whether to dry-run (instead of actually running). If enabled, no AWS resources will be provisioned, changed, or deleted.",
    )

    parser.add_argument(
        "--endpoint-url",
        type=str,
        required=False,
        default=None,
        help="The endpoint URL of the AWS service to use. If not specified, the default endpoint for the service will be used. This is mostly useful when using localstack for testing.",
    )

    args = parser.parse_args()

    grandiso = GrandIso(
        endpoint_url=args.endpoint_url,
        aws_access_key_id="grandiso",
        aws_secret_access_key="grandiso",
        dry=True,
    )

    subparsers = parser.add_subparsers(help="Commands.")

    # Provision
    provision_command = subparsers.add_parser(
        "provision", help="Provision resources for GrandIso."
    )
    provision_command.add_argument(
        "--batch-size",
        type=int,
        required=False,
        default=1,
        help="The batch size for the SQS event trigger.",
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
            + "information (a.weight > 5) will be omitted."
        ),
    )
    kickoff_command.add_argument(
        "--directed",
        type=bool,
        default=True,
        help="Whether to assume directed motif and host graphs.",
    )
    kickoff_command.add_argument(
        "--job",
        type=str,
        required=True,
        help=(
            "The job name to use to refer to this subgraph isomorphism "
            + "search. Make one up when calling `provision`, and then reuse "
            + "the same name for the other commands."
        ),
    )
    kickoff_command.set_defaults(func=grandiso.cli_kickoff)

    # Results
    results_command = subparsers.add_parser(
        "results", help="Print the results from the search."
    )
    results_command.add_argument(
        "--job",
        type=str,
        required=True,
        help=(
            "The job name to use to refer to this subgraph isomorphism "
            + "search. Make one up when calling `provision`, and then reuse "
            + "the same name for the other commands."
        ),
    )
    results_command.add_argument(
        "--format",
        default="csv",
        type=str,
        required=False,
        help="The format (csv|json) in which to return results.",
    )
    results_command.set_defaults(func=grandiso.print_cli_results)

    # Parse args:
    args = parser.parse_args()
    grandiso.dry = args.dry
    args.func(args)


if __name__ == "__main__":

    cli_main()
