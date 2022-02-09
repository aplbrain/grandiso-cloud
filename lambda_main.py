"""
This script is a single worker node for the distributed cloud motif-search
implementation. The `handle_queue_element` function is called with a payload of
an individual job.

The handle_queue_multijob function is called by a linked SQS queue, with the
potential for a payload of multiple jobs. This function is called automatically
and is not intended to be called manually.

There are two user-definable functions (specified in the CONFIG object) that
handle results and requeueing, respectively.

All functions take as their first parameter a job_id

"""

from typing import Callable

import boto3
from grandiso import get_next_backbone_candidates, uniform_node_interestingness
import networkx as nx
from configuration import configuration

ResultHandlerFunction = Callable[[str, dict], bool]


config = configuration()

dynamo_client = boto3.client("dynamodb")


def save_result_to_dynamo(job_id: str, result_payload: dict) -> bool:
    """
    Save a single result to DynamoDB.

    Arguments:
        job_id (str): The job ID of the current result, to associate it with a
            grand iso cloud job.
        result_payload (dict): The result payload to save, including the iso
            mapping.

    Returns:
        bool: True if the result was saved successfully, False otherwise.

    """
    try:
        dynamo_client.put_item(
            TableName="motif-search-results",
            Item={
                "job_id": {"S": job_id},
                "result": {"S": str(result_payload)},
                "timestamp": {"S": str(result_payload["timestamp"])},
            },
        )
        return True
    except Exception as e:
        print(f"Error saving result to dynamo: {e}")
        return False


def process_one_partial_candidate_from_queue(
    queue_payload: dict, results_handler: ResultHandlerFunction
) -> bool:
    """
    Process a single partial candidate from the queue.

    Arguments:
        queue_payload (dict): The payload of the queue element to process.
        results_handler (ResultHandlerFunction): The function to call to
            handle the results of the job.

    Returns:
        bool: True if the job was processed successfully, False otherwise.

    """
    job_id = queue_payload["job_id"]
    motif = queue_payload["motif"]
    candidate = queue_payload["candidate"]
    directed = queue_payload["directed"]
    interestingness = queue_payload.get("interestingness", None)

    motif_nx = nx.readwrite.node_link_graph(motif, directed=directed)
    interestingness = interestingness or uniform_node_interestingness(motif_nx)

    new_candidates = get_next_backbone_candidates(
        candidate,
        motif_nx,
        host,
        interestingness=interestingness,
        directed=directed,
    )

    result_payload["job_id"] = job_id
    result_payload["timestamp"] = queue_payload["timestamp"]

    if results_handler(job_id, result_payload):
        return True
    else:
        return False
