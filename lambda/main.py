import json
import os

import boto3
import networkx as nx

from grand.backends import DynamoDBBackend
import grand

from grandiso import get_next_backbone_candidates, uniform_node_interestingness

"""
def get_next_backbone_candidates(
    backbone: dict,
    motif: nx.Graph,
    host: nx.Graph,
    interestingness: dict,
    next_node: str = None,
    directed: bool = True,
    enforce_inequality: bool = True,
) -> List[dict]:
"""

# LOCALSTACK_HOSTNAME = os.getenv("LOCALSTACK_HOSTNAME")
LOCALSTACK_HOSTNAME = "172.17.0.1"
LOCALSTACK_HOSTNAME = "localhost"

ENDPOINT_URL = "http://" + LOCALSTACK_HOSTNAME + ":4566"


def find_motifs_step(
    motif: nx.Graph, backbone: dict, host: grand.Graph, interestingness=None
):
    interestingness = interestingness or uniform_node_interestingness(motif)

    if isinstance(motif, nx.DiGraph):
        # This will be a directed query.
        directed = True
    else:
        directed = False

    next_candidate_backbones = get_next_backbone_candidates(
        backbone, motif, host, interestingness, directed=directed
    )

    results = []
    for candidate in next_candidate_backbones:
        if len(candidate) == len(motif):
            results.append(candidate)
        else:
            # q.put(candidate)
            print(candidate)

    print(results)
    return results


def main(event, lambda_context):

    DG = grand.Graph(backend=DynamoDBBackend(dynamodb_url=ENDPOINT_URL))

    print(len(DG.nx))

    # sqs_client = boto3.client(
    #     "sqs",
    #     use_ssl=False,
    #     endpoint_url=ENDPOINT_URL,
    #     aws_access_key_id="foo",
    #     aws_secret_access_key="foo",
    #     region_name="us-east-1",
    # )

    # print("!!!")

    candidate = json.loads(event["Records"][0]["body"])
    # if candidate["foo"] == "bar":
    #     print("!!! foo is bar")
    #     queue_url = sqs_client.get_queue_url(QueueName="GrandIsoQ")["QueueUrl"]
    #     print("!!! got URL")
    #     sqs_client.send_message(QueueUrl=queue_url, MessageBody='{"foo": "baz"}')
    #     print("!!! sent message")
    #     result = json.dumps({"status": "queueing..."})
    #     print(result)
    #     return result

    result = json.dumps({"candidate": candidate})
    print(result)
    return result


if __name__ == "__main__":
    main(0, 0)
