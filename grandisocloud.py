#!/usr/bin/env python3

"""
This is a single worker node that can be run against a local or cloud queue.

"""

from fire import Fire
from functools import partial
from taskqueue import queueable, TaskQueue

import networkx as nx

from grand.backends import SQLBackend
import grand

from grandiso import (
    uniform_node_interestingness,
    get_next_backbone_candidates,
)


MOTIF = nx.DiGraph()
MOTIF.add_edge("1", "2")
MOTIF.add_edge("2", "3")
MOTIF.add_edge("3", "1")

interestingness = uniform_node_interestingness(MOTIF)


@queueable
def get_next_backbone_candidates_and_enqueue(
    job_id: str, queue_uri: str, candidate: dict, host_grand_uri: str
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
    q = TaskQueue(queue_uri)

    # get the next backbone candidates
    next_candidates = get_next_backbone_candidates(
        candidate, MOTIF, host, interestingness=interestingness, directed=False
    )
    for c in next_candidates:
        if len(c.keys()) == len(MOTIF.nodes()):
            # TODO: Handle complete records
            pass
        else:
            q.insert(
                [
                    partial(
                        get_next_backbone_candidates_and_enqueue,
                        job_id,
                        queue_uri,
                        c,
                        host_grand_uri,
                    )
                ]
            )


def initialize(job_id: str, queue_uri: str, host_grand_uri: str) -> None:
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
    get_next_backbone_candidates_and_enqueue(job_id, queue_uri, {}, host_grand_uri)


def run(
    job_id: str,
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
        job_id (str): The job ID to run.
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
