# Grand Isomorphisms

Grand is a virtual graph database. Because DynamoDB is a true-serverless database, it makes sense to use serverless scalable technologies to run graph queries against Grand.

In particular, subgraph isomorphism is a resource-heavy (but branch-parallelizable) algorithm that is hugely impactful for large graph analysis. SotA algorithms for this (Ullmann, VF2, BB-Graph) are heavily RAM-bound, but this is due to a large number of small processes each of which hold a small portion of a traversal tree in memory.

_Grand-Iso_ is a subgraph isomorphism algorithm that leverages serverless technology to run in the AWS cloud at infinite scale.\*

> <small>\* You may discover that "infinite" here is predominantly bounded by your wallet, which is no joke.</small>

For an overview of the algorithm and cloud architecture at work here, see [docs/algorithm.md](docs/algorithm.md).

# Preparation

## Prerequisites

You should confirm that you have set `AWS_DEFAULT_REGION` somewhere. `us-east-1` is recommended. You will also need an IAM user (or user account) with access to the following services:

-   DynamoDB
-   IAM
-   Lambda
-   SQS

I recommend saving this configuration in `~/.aws/credentials` and then referencing with the `AWS_PROFILE` environment variables, though you can pass credentials directly (or as environment variables) as well.

## Installing Dependencies

In order to use this package, you will need the libraries listed in `requirements.txt`. You can either install them manually, or you can install them from the `requirements.txt` in this directory:

```shell
pip install -r requirements.txt
```

You will also need to install dependencies for the lambda runtime. The script `lambda/downloadvendor.sh` will handle this for you automatically. Note that if you change which versions of libraries you are using, you will need to make sure that any installed libraries with compiled binaries (e.g. numpy, pandas) are compatible with a 64-bit linux system. (If you are using the `downloadvendor.sh` script, you can ignore this warning; the versions are correct already.)

To install vendored libraries, run the following (you must run the script from inside the `lambda` directory):

```shell
cd lambda
bash ./downloadvendor.sh
cd ..
```

Your repository should now look like:

```
+ (this repository)/
|
+-+ lambda/
| |
| +-+ downloadvendor.sh
| |
| +-+ main.py
| |
| +-+ vendor/
|   |
|   +-+ ...
|   +-+ (python packages)
|   +-+ ...
|
+ (this readme)
```

# Optional: Localstack configuration

For testing and development purposes, you can use `localstack` to act as a mock for the AWS cloud environment. Note that performance is dramatically slower and there is a severe overhead penalty, so you shold avoid using localstack for production or larger workloads.

To run localstack, we recommend downloading the latest version from GitHub, and running with `docker-compose`:

```shell
git clone https://github.com/localstack/localstack
cd localstack
AWS_PROFILE='localstack' TMPDIR=/private$TMPDIR DEBUG=1 SERVICES=serverless,cloudformation,sqs,events PORT_WEB_UI=8082 docker-compose up
```

If you are using localstack, you should specify all endpoint URLs in the steps below, with the `--endpoint-url` command line argument.

# Usage

You are now ready to begin using this package. All of the following commands can be run with `--dry` or `--dry=true` to prevent them from affecting your AWS account:

```shell
./gi_manage.py --dry=true --job "Example" provision
```

## Global Flags

| Flag             | Description                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------ |
| `--dry`          | If true, the command will not actually run, but will show the commands that would have been run. |
| `--endpoint-url` | The endpoint URL to use for the AWS API.                                                         |

Start by provisioning your resources:

## Provision resources for the first time

You can choose the batch-size (how many elements are popped from the queue to be run in series in the worker lambda). This should be a small number (perhaps even 1) if the graph is particularly large, to guarantee that the Lambda completes without timing out. If the graph is small, or the motif is very sparse, you can set this to a large number (e.g. 20).

If a batch size is not provided, `gi_manage.py` will default to a batch-size of 1.

```shell
./gi_manage.py provision --batch_size=10
```

This will do two things:

-   Make sure you have all Lambdas, Queues, and Results-tables provisioned in Lambda, SQS, and DynamoDB
-   Connect your GrandIso lambda to listen for incoming messages to your SQS Queue

## Kick off the job

```shell
./gi_manage.py kickoff --job MyCoolJob --motif mymotif.motif
```

## Get the results

Note that you can request results right away, but the job may not be finished yet! (You'll get an incomplete set of results, but they will all be valid mappings.)

Note that this performs a seriaized `DynamoDB#scan` operation, which is costly on a sufficiently large table!

```shell
./gi_manage.py results --job MyCoolJob --format csv > myresults.csv
```

Complete reference documentation for the `gi_manage.py` file exists in [docs/reference.md](docs/reference.md).
