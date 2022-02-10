"""
This is a single worker node that can be run against a local or cloud queue.

"""
from functools import partial
from taskqueue import queueable, TaskQueue
from grandiso import (
    uniform_node_interestingness,
    get_next_backbone_candidates,
)

JOB_ID = "count77"
QUEUE_URI = "fq://demoqueue"
LEASE_SECONDS = 5


Q = TaskQueue(QUEUE_URI)

import networkx as nx

HOST = nx.read_graphml("./witvliet.8.graphml")
MOTIF = nx.Graph()
MOTIF.add_edge("1", "2")
MOTIF.add_edge("2", "3")
MOTIF.add_edge("3", "1")

interestingness = uniform_node_interestingness(MOTIF)


@queueable
def get_next_backbone_candidates_and_enqueue(job_id: str, candidate: dict):

    # get the next backbone candidates
    next_candidates = get_next_backbone_candidates(
        candidate, MOTIF, HOST, interestingness=interestingness, directed=False
    )
    for c in next_candidates:
        if len(c.keys()) == len(MOTIF.nodes()):
            print(c)
        else:
            Q.insert([partial(get_next_backbone_candidates_and_enqueue, job_id, c)])


if __name__ == "__main__":
    # print(find_motifs(MOTIF, HOST, count_only=True))
    get_next_backbone_candidates_and_enqueue(JOB_ID, {})

    Q.poll()
