## Pseudocode for novel "Grand-Iso" algorithm

```
- Provision a DynamoDB table for result storage.
- Preprocessing
    - Identify highest-degree node in motif, M1
    - Identify second-highest degree node in motif, M2, connected to M1 by
        a single edge.
    - Identify all nodes with degree of M1 or greater in the host graph,
        which also have all required attributes of the M1 and M2 nodes. If
        neither M1 nor M2 have degree > 1 nor attributes, select M1 and M2 as
        two nodes with attributes defined.
    - Enumerate all paths in the host graph from M1 candidates to M2 candidates,
        as candidate "backbones" in AWS SQS.
- Motif Search
    - For each backbone candidate:
        - Schedule an AWS Lambda:
            - Pop the backbone from the queue.
            - Traverse all shortest paths in the motif starting at the nearest
                of either M1 or M2
            - If multiple nodes are valid candidates, queue a new backbone with
                each option, and terminate the current Lambda.
            - When all paths are valid paths in the host graph, add the list
                of participant nodes to a result in the DynamoDB table.
- Reporting
    - Return a serialization of the results from the DynamoDB table.
- Cleanup
    - Delete the backbone queue
    - Delete the results table (after collection)
```
