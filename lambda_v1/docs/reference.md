## _Class_ `PermissiveZipFile(ZipFile)`

A Zipfile class that also sets file permission flags.

This is required for the lambda runtime. Thanks to the answerers at https://stackoverflow.com/questions/434641/ for insights and useful code.

## _Function_ `_generate_zip(vendor_directories: list = None, include_files: list = None)`

Construct a zip file from the vendored and nonvendored libraries.

## _Function_ `zipdir(dpath, zipf)`

Zip an entire directory, recursively.

## _Class_ `Motif`

A class to manage reading motifs in a variety of formats.

## _Function_ `__init__(self, graph: nx.Graph) -> None`

Create a new Motif from a file, graph, or other structure.

### Arguments

> -   **graph** (`nx.Graph`: `None`): A source graph to turn into a motif (using the

        DotMotif library)

## _Function_ `from_file(filename: str, directed: bool = True) -> "Motif"`

Create a new Motif object from a .motif file or .csv file.

### Arguments

> -   **filename** (`str`: `None`): The file path to read
> -   **True)** (`None`: `None`): If the file details a directed (True) or

        undirected (False) motif

### Returns

    Motif

## _Function_ `from_motif(filename: str, directed: bool = True) -> "Motif"`

Create a new Motif object from a .motif file.

### Arguments

> -   **filename** (`str`: `None`): The file path to read
> -   **True)** (`None`: `None`): If the file details a directed (True) or

        undirected (False) motif

### Returns

    Motif

## _Function_ `from_edgelist(filename: str, directed: bool = True) -> "Motif"`

Render a .csv file to a networkx.Graph.

### Arguments

> -   **filename** (`str`: `None`): The file path to read
> -   **True)** (`None`: `None`): If the file details a directed (True) or

        undirected (False) motif

### Returns

    Motif

## _Function_ `to_nx(self) -> nx.Graph`

Export the Motif object to its underlying networkx.Graph.

### Arguments

    None

### Returns

> -   **nx.Graph** (`None`: `None`): The graph representation of the motif.

## _Function_ `_scan_table(table: boto3.Resource, scan_kwargs: dict = None)`

DynamoDB convenience function to scan all results from a table.

### Arguments

> -   **table** (`boto3.Resource`: `None`): The boto3 DynamoDB Table object to use for

        database operations

> -   **None)** (`None`: `None`): Arbitrary additional keyword arguments to

        pass to the Table#scan operation

### Returns

    List

## _Function_ `_dynamo_table_exists(table_name: str, client: boto3.client)`

DynamoDB convenience function to check if a DynamoDB table already exists.

### Arguments

> -   **table_name** (`str`: `None`): The name of the table to check
> -   **client** (`boto3.client`: `None`): The boto3 DynamoDB client

### Returns

> -   **bool** (`None`: `None`): Whether table exists

## _Class_ `GrandIso`

A class responsible for creating, managing, and manipulating GrandIso jobs in the cloud.

Certain resources, like the Lambda and SQS queue, are reused across all of the GrandIso jobs that you run on the same account. Other resources, like the results table in DynamoDB, are specific to individual jobs.

## _Function_ `teardown(self)`

Tear down all resources associated with GrandIso in this account.

Note that this can take several minutes, and specifically when deleting a SQS Queue, you may not be able to create a new Queue with the same name until several minutes have passed.

If you are trying to start fresh, you should NOT call the GrandIso teardown function, and instead you should remove the resources specific to your job.

### Arguments

    None

### Returns

    None

## _Function_ `cli_teardown(self, argparser_args=None)`

Run the teardown command from the command-line.

See GrandIso#teardown.

## _Function_ `cancel(self)`

Cancel all GrandIso jobs in this account.

This will cancel the jobs but not delete the AWS resources.

### Arguments

    None

### Returns

    None

## _Function_ `aggregate_results(self, job: str) -> Iterable`

Get the results of a GrandIso job.

### Arguments

> -   **job** (`str`: `None`): The job to get results for

### Returns

> -   **Iterable** (`None`: `None`): The results of the job

## _Function_ `cli_results(self, argparser_args=None)`

Run the results command from the command-line.

See GrandIso#aggregate_results.

## _Function_ `print_cli_results(self, argparser_args=None)`

Run the results command from the command-line, and print the results.

See GrandIso#cli_results.

## _Function_ `create_queue(self) -> str`

Create a new SQS Queue.

### Arguments

    None

### Returns

> -   **str** (`None`: `None`): The ARN URI of the new queue

## _Function_ `create_lambda(self) -> str`

Create a lambda for GrandIso in general.

Note that this is not specific to a job, and so it is not prefixed with namespacing variables.

### Arguments

    None

### Returns

> -   **str** (`None`: `None`): The ARN URI of the new lambda

## _Function_ `create_table(self) -> Union[str, bool]`

Create a DynamoDB table to hold the results from this job.

### Arguments

    None

### Returns

> -   **str** (`None`: `None`): The ARN URI of the new table, or False if the table already

        exists and does not need to be created.

## _Function_ `attach_queue_event(self, queue_arn: str, lambda_arn: str)`

Attach the SQS new-item event handler to the lambda.

### Arguments

> -   **queue_arn** (`str`: `None`): The ARN of the SQS queue
> -   **lambda_arn** (`str`: `None`): The ARN of the Lambda function

### Returns

    None

## _Function_ `provision(self) -> Tuple[str, str, str]`

Create the queue, lambda, and table for Grand Iso cloud jobs.

### Arguments

    None

### Returns

> -   **str]** (`None`: `None`): The ARN of the queue, the ARN of the lambda,

        and the ARN of the table (in that order).

## _Function_ `cli_provision(self, argparser_args=None)`

Run the provision command from the command-line.

See GrandIso#provision.

## _Function_ `queue_push(self, value: str)`

Push a new item to the queue.

### Arguments

> -   **value** (`str`: `None`): The value to push to the queue.

### Returns

    None

## _Function_ `cli_kickoff(self, argparser_args=None)`

Kick off a new job from the command-line.

See GrandIso#kickoff.

## _Function_ `cli_main()`

Run the command-line interface.

### Arguments

    None

### Returns

    None
