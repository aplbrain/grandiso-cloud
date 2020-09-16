## Create a host graph

If you already have a host graph available (uploaded with `Grand`), you can skip this step.

```python
import grand
from grand.backends import DynamoDBBackend

G = grand.Graph(backend=DynamoDBBackend(dynamodb_url=self.endpoint_url))

G.nx.add_edge("A", "B")
G.nx.add_edge("B", "C")
G.nx.add_edge("C", "A")
```
