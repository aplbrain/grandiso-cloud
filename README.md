# GrandIso Cloud

This is a cloud-queue implementation of the [GrandIso](https://github.com/aplbrain/grandiso-networkx) library for motif search at cloud-scale.

## Motivation

Subgraph monomorphism, the underlying algorithm behind motif search, is memory-hungry, and most state of the art implementations may require the allocation of many terabytes of RAM even for relatively small host graphs (thousands to tens-of-thousands of edges). The GrandIso algorithm isolates this memory cost in a single, one-dimensional queue data structure. In the original GrandIso tool (written for the [NetworkX](https://github.com/networkx/networkx) Python library), the queue resides in memory. This makes it extremely fast, but it also means that the total size of the motif search task is limited by the RAM of the machine. This is a particular issue for modern large-scale graph analysis questions, where the host graphs may exceed hundreds of millions of edges. Such a graph would exceed the memory- and time-budgets of most institutions. Thus, there are two main limitations to consider: (1) The size of the raw graph data may exceed what can be stored on a single machine; (2) The RAM requirements of the queue may exceed the RAM of the machine.

### Solutions for Large-Scale Graphs

This implementation uses [Grand](https://github.com/aplbrain/grand), a Python library that serves a NetworkX-like API in front of graph data stored in SQL, DynamoDB, Neo4j, or other database technologies. This enables a user to operate on larger-than-RAM graphs while writing familiar and readable code. In other words, a user never needs to learn graph database APIs or query optimizations, and can still manipulate million-edge graph data efficiently.

### Solutions for Out-of-Memory Queue Management

This cloud implementation outsources queue management to a dropout-resilient queue system like [AWS SQS](https://aws.amazon.com/sqs/). This has three main advantages: First, it removes local RAM from the equation; the queue can scale exponentially on the remote cloud host without impacting local performance. Second, it enables multiple parallel workers — on the same machine or on multiple machines — to work cooperatively on the same graph. And finally, such a queue adds a layer of resilience to drop-out or node death, which enables the user to recruit much larger clusters of unsupervised worker nodes.

## Considerations

This performant software greatly reduces the total wall-clock time to perform motif searches on large graphs. Do note, however, that this is due to the use of cloud data hosting and massive parallelization. Both of these aspects have nontrivial costs associated with them, and it is recommended that users use this software with caution and budget-awareness. (It is NOT difficult to accidentally spawn a cost-prohibitive motif search using this infrastructure!)

It is recommended that users of this system perform some small benchmarking searches prior to launching full-scale motif searches.

## Usage

Many jobs may be spawned simultaneously. Each job, whether or not running in isolation, must be assigned a unique **job ID**. This job ID is used to identify the job in the queue, and is also used to identify the job's output. (Reusing a job ID is legal, but may corrupt your results or make them uninterpretable. This software will not stop you from doing this.)

### Initialization

To create a new motif search job, run the following:

```shell
./grandisocloud.py init \
    --job-id <job-id> \
    --queue-url <queue-url> \
    --host-grand-uri <host-grand-uri>
```

The three required arguments are:

| Argument           | Description                                    | Example                                    |
| ------------------ | ---------------------------------------------- | ------------------------------------------ |
| `--job-id`         | A unique identifier for the job.               | `--job-id='my-fun-job'`                    |
| `--queue-url`      | The python-task-queue URI of the queue to use. | `--queue-url='fq://my-grandiso-queue'`     |
| `--host-grand-uri` | The location of the Grand Graph.               | `--host-grand-uri='sqlite:///my-graph.db'` |

**IMPORTANT:** Note that initialization must have access to the Grand Graph, and will perform a full database table scan in order to populate the queue with initial candidate partial motif matches. This may take a long time for large graphs, and may incur significant costs if your graph has hundreds of millions or billions of edges.

### Attaching a worker to a job

Jobs are completed by attaching one or more workers. A worker can be attached to a job by running the following:

```shell
./grandisocloud.py run \
    --job-id <job-id> \
    --queue-url <queue-url>
```

Note that you do not need to specify a host URI. (This information is stored in the queue job.)
