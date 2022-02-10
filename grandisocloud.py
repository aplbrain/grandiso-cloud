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
):

    host = grand.Graph(
        backend=SQLBackend(db_url=host_grand_uri, directed=True),
        directed=True,
    ).nx
    q = TaskQueue(queue_uri)

    # get the next backbone candidates
    next_candidates = get_next_backbone_candidates(
        candidate, MOTIF, host, interestingness=interestingness, directed=False
    )
    print(next_candidates)
    for c in next_candidates:
        if len(c.keys()) == len(MOTIF.nodes()):
            print(c)
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


def initialize(job_id: str, queue_uri: str, host_grand_uri: str):
    get_next_backbone_candidates_and_enqueue(job_id, queue_uri, {}, host_grand_uri)


def run(job_id: str, queue_uri: str):
    Q = TaskQueue(queue_uri)
    Q.poll(verbose=True)


if __name__ == "__main__":
    Fire(
        {
            "init": initialize,
            "run": run,
        }
    )
