#!/usr/bin/env python3

"""
This is a single worker node that can be run against a local or cloud queue.

"""
import json
import pathlib
from typing import Any, Callable, Dict
from functools import partial

from fire import Fire
from grand.backends import SQLBackend
import grand
from grandiso import (
    uniform_node_interestingness,
    get_next_backbone_candidates,
)
import networkx as nx
from taskqueue import queueable, TaskQueue

# Type aliases
JobID = str
CandidateMapping = Dict[str, str]
ResultHandlerFunction = Callable[[JobID, CandidateMapping, Any], Any]


def append_file_result_handler(
    job_id: str, candidate_mapping: CandidateMapping, filename: str
) -> None:
    """
    Push the result of a job to the queue.

    Arguments:
        job_id (str): The job ID to push the result for.
        candidate_mapping (CandidateMapping): The candidate mapping to push.
        filename (str): The filename to write the result to.

    Returns:
        None

    """
    fname = f"{filename}-{job_id}"
    # append a single binary byte to the file to indicate that the job is complete.
    with open(fname, "ab") as f:
        f.write(b"\x00")


RESULT_HANDLER: ResultHandlerFunction = append_file_result_handler


@queueable
def get_next_backbone_candidates_and_enqueue(
    job_id: str,
    queue_uri: str,
    candidate: dict,
    motif_json: dict,
    interestingness: dict,
    host_grand_uri: str,
) -> None:
    """
    Perform a single iteration of the algorithm.

    Retrieves a job from the queue, expands upon it using GrandIso, and then
    enqueues the resulting candidates.

    Arguments:
        job_id (str): The job ID of the job to expand upon.
        queue_uri (str): The URI of the queue to use. See ptq for more details.
        candidate (dict): The candidate to expand upon. (As an end-user, you
            will not have to interact with this directly.) To start a new run,
            pass an empty dict.
        host_grand_uri (str): The URI of the host graph to use (see grand docs
            for more details).

    Returns:
        None

    """
    host = grand.Graph(
        backend=SQLBackend(db_url=host_grand_uri, directed=True),
        directed=True,
    ).nx
    motif = nx.readwrite.json_graph.node_link_graph(motif_json)
    q = TaskQueue(queue_uri)

    # get the next backbone candidates
    next_candidates = get_next_backbone_candidates(
        candidate,
        motif,
        host,
        interestingness=interestingness,
        directed=motif.is_directed(),
    )
    for c in next_candidates:
        if len(c) == len(motif):
            RESULT_HANDLER(job_id, c, job_id)
        else:
            q.insert(
                [
                    partial(
                        get_next_backbone_candidates_and_enqueue,
                        job_id,
                        queue_uri,
                        c,
                        motif_json,
                        interestingness,
                        host_grand_uri,
                    )
                ]
            )


def initialize(
    job_id: str, queue_uri: str, host_grand_uri: str, motif_json: str
) -> None:
    """
    Initialize a new GrandIsoCloud job.

    Arguments:
        job_id (str): The job ID to create. Will not be checked for uniqueness.
        queue_uri (str): The URI of the queue to use. See ptq for more details.
        host_grand_uri (str): The URI of the host graph to use (see grand docs
            for more details).

    Returns:
        None

    """
    resolved_path = pathlib.Path(motif_json).expanduser().resolve()
    if resolved_path.exists():
        with open(resolved_path, "r") as f:
            motif_json = json.load(f)
    else:
        try:
            motif_json = json.loads(motif_json)
            nx.readwrite.json_graph.node_link_graph(motif_json)
        except ValueError:
            raise ValueError(
                "Could not parse motif JSON. Please ensure that the JSON is valid."
            )

    motif = nx.readwrite.json_graph.node_link_graph(motif_json)

    interestingness = uniform_node_interestingness(motif)

    get_next_backbone_candidates_and_enqueue(
        job_id, queue_uri, {}, motif_json, interestingness, host_grand_uri
    )


def run(
    queue_uri: str,
    verbose: bool = False,
    lease_seconds: int = None,
    tally: bool = False,
) -> None:
    """
    Run a GrandIsoCloud job.

    Attaches to an existing work queue. ALL jobs in the queue will be processed
    by this worker, so if you have multiple jobs with different host graphs,
    make sure they are all available and visible to each worker. (Otherwise,
    use different queue URIs for each host graph.)

    If you have multiple workers, you can use the same queue URI for each.

    Arguments:
        queue_uri (str): The URI of the queue to use. See ptq for more details.
        verbose (bool): Whether to print progress information.
        lease_seconds (int): The number of seconds to wait for a job before
            timing out.
        tally (bool): Whether to keep a tally of the number of jobs processed.

    Returns:
        None

    """
    Q = TaskQueue(queue_uri)
    if lease_seconds:
        Q.poll(
            verbose=verbose,
            tally=tally,
            lease_seconds=lease_seconds,
        )
    else:
        Q.poll(verbose=verbose, tally=tally)


if __name__ == "__main__":
    Fire(
        {
            "init": initialize,
            "run": run,
        }
    )
