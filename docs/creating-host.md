## Create a host graph

If you already have a host graph available (uploaded with `Grand`), you can skip this step.

```python
import grand
from grand.backends import DynamoDBBackend

G = grand.Graph(backend=DynamoDBBackend(), directed=True)

G.nx.add_edge("A", "B")
G.nx.add_edge("B", "C")
G.nx.add_edge("C", "A")
G.nx.add_edge("A", "D")
G.nx.add_edge("D", "C")
```

You can also point to `localstack` DynamoDB endpoints like so:

```python
G = grand.Graph(
    backend=DynamoDBBackend(dynamodb_url="http://localhost:4566"),
    directed=True
)
```
