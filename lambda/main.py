import json
import os
from uuid import uuid4

import boto3
import networkx as nx

from grand.backends import DynamoDBBackend
import grand

from grandiso import get_next_backbone_candidates, uniform_node_interestingness

# LOCALSTACK_HOSTNAME = os.getenv("LOCALSTACK_HOSTNAME")
LOCALSTACK_HOSTNAME = "172.17.0.1"
# LOCALSTACK_HOSTNAME = "localhost"

ENDPOINT_URL = "http://" + LOCALSTACK_HOSTNAME + ":4566"

sqs_client = boto3.client(
    "sqs",
    use_ssl=False,
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="foo",
    aws_secret_access_key="foo",
    region_name="us-east-1",
)
queue_url = sqs_client.get_queue_url(QueueName="GrandIsoQ")["QueueUrl"]

dynamodb_resource = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id="foo",
    aws_secret_access_key="foo",
)
results_table = dynamodb_resource.Table("GrandIsoResults")


def find_motifs_step(
    motif: dict, backbone: dict, host: grand.Graph, interestingness=None
):
    motif_nx = nx.readwrite.node_link_graph(motif)
    interestingness = interestingness or uniform_node_interestingness(motif_nx)

    if isinstance(motif_nx, nx.DiGraph):
        # This will be a directed query.
        directed = True
    else:
        directed = False

    next_candidate_backbones = get_next_backbone_candidates(
        backbone, motif_nx, host, interestingness, directed=directed
    )

    results = []
    for candidate in next_candidate_backbones:
        if len(candidate) == len(motif_nx):
            results_table.put_item(
                Item={"candidate": candidate, "motif": motif, "ID": str(uuid4())}
            )
            # results.append(candidate)
        else:
            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(
                    {"motif": motif, "candidate": candidate, "ID": str(uuid4())}
                ),
            )
            print(candidate)

    print(results)
    return results


def main(event, lambda_context):

    payload = json.loads(event["Records"][0]["body"])

    DG = grand.Graph(backend=DynamoDBBackend(dynamodb_url=ENDPOINT_URL))

    print(find_motifs_step(payload["motif"], payload["candidate"], DG.nx))


# if __name__ == "__main__":
#     motif = nx.DiGraph()
#     motif.add_edge("A", "B")
#     motif.add_edge("B", "C")
#     motif.add_edge("C", "A")

#     main({"motif": nx.readwrite.node_link_data(motif), "candidate": {}}, None)
