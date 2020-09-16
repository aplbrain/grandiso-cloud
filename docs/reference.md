## *Class* `PermissiveZipFile(ZipFile)`


A Zipfile class that also sets file permission flags.

This is required for the lambda runtime. Thanks to the answerers at https://stackoverflow.com/questions/434641/ for insights and useful code.



## *Function* `_dynamo_table_exists(table_name: str, client: boto3.client)`


Check to see if the DynamoDB table already exists.

### Returns
> - **bool** (`None`: `None`): Whether table exists



## *Class* `GrandIso`


A class responsible for creating, managing, and manipulating GrandIso jobs in the cloud.

Certain resources, like the Lambda and SQS queue, are reused across all of the GrandIso jobs that you run on the same account. Other resources, like the results table in DynamoDB, are specific to individual jobs.



## *Function* `teardown(self)`


Tear down all resources associated with GrandIso in this account.

Note that this can take several minutes, and specifically when deleting a SQS Queue, you may not be able to create a new Queue with the same name until several minutes have passed.

If you are trying to start fresh, you should NOT call the GrandIso teardown function, and instead you should remove the resources specific to your job.



## *Function* `cli_teardown(self, argparser_args=None)`


Run the teardown command from the command-line.

See GrandIso#teardown.



## *Function* `generate_zip(self)`


Construct a zip file from the vendored and nonvendored libraries.



## *Function* `zipdir(dpath, zipf)`


Zip an entire directory, recursively.


## *Function* `create_lambda(self)`


Create a lambda for GrandIso in general.

Note that this is not specific to a job, and so it is not prefixed with namespacing variables.



## *Function* `create_table(self)`


Create a DynamoDB table to hold the results from this job.


