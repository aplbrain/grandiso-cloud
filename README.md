# GrandIso Cloud

This is a cloud-queue implementation of the [GrandIso](https://github.com/aplbrain/grandiso-networkx) library for motif search at cloud-scale.

## Motivation

Subgraph monomorphism, the underlying algorithm behind motif search, is memory-hungry, and most state of the art implementations may require the allocation of many terabytes of RAM even for relatively small host graphs (thousands to tens-of-thousands of edges). The GrandIso algorithm isolates this memory cost in a single, one-dimensional queue data structure. In the original GrandIso tool (written for the [NetworkX](https://github.com/networkx/networkx) Python library), the queue resides in memory. This makes it extremely fast, but it also means that the total size of the motif search task is limited by the RAM of the machine. This is a particular issue for modern large-scale graph analysis questions, where the host graphs may exceed hundreds of millions of edges. Such a graph would exceed the memory- and time-budgets of most institutions. Thus, there are two main limitations to consider: (1) The size of the raw graph data may exceed what can be stored on a single machine; (2) The RAM requirements of the queue may exceed the RAM of the machine.

### Solutions for Large-Scale Graphs

This implementation uses [Grand](https://github.com/aplbrain/grand), a Python library that serves a NetworkX-like API in front of graph data stored in SQL, DynamoDB, Neo4j, or other database technologies. This enables a user to operate on larger-than-RAM graphs while writing familiar and readable code. In other words, a user never needs to learn graph database APIs or query optimizations, and can still manipulate million-edge graph data efficiently.

### Solutions for Out-of-Memory Queue Management

This cloud implementation outsources queue management to a dropout-resilient queue system like [AWS SQS](https://aws.amazon.com/sqs/). This has three main advantages: First, it removes local RAM from the equation; the queue can scale exponentially on the remote cloud host without impacting local performance. Second, it enables multiple parallel workers — on the same machine or on multiple machines — to work cooperatively on the same graph. And finally, such a queue adds a layer of resilience to drop-out or node death, which enables the user to recruit much larger clusters of unsupervised worker nodes.
