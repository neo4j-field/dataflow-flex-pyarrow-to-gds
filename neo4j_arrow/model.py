"""
--------------------------------------------------------------
neo4j_arrow.model: A graph model for mapping over source data.
--------------------------------------------------------------
"""

from json import dumps, loads, JSONEncoder

from typing import Any, Dict, Generic, List, Union, TypeVar


class _NodeEncoder(JSONEncoder):
    def default(self, n: 'Node'):
        return n.to_dict()


class _EdgeEncoder(JSONEncoder):
    def default(self, e: 'Edge'):
        return e.to_dict()


class _GraphEncoder(JSONEncoder):
    def default(self, g: 'Graph'):
        return g.to_dict()


class Node:
    def __init__(self, *, source: str, label: str = "", label_field: str,
                 key_field: str, **properties: Dict[str, Any]):
        self._source = source
        self._label = label
        self._label_field = label_field
        self._key_field = key_field
        self._properties = properties

    @property
    def source(self) -> str:
        return self._source

    @property
    def label(self) -> str:
        return self._label

    @property
    def key_field(self) -> str:
        return self._key_field

    @property
    def label_field(self) -> str:
        return self._label_field

    @property
    def properties(self) -> Dict[str, Any]:
        return self._properties

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self._source,
            "label": self._label,
            "label_field": self._label_field,
            "key_field": self._key_field,
            "properties": self._properties,
        }

    def __str__(self) -> str:
        return str(self.to_dict())


class Edge:
    def __init__(self, *, source: str, edge_type: str = "", type_field: str,
                 source_field: str, target_field: str,
                 **properties: Dict[str, Any]):
        self._source = source
        self._type = edge_type
        self._type_field = type_field
        self._source_field = source_field
        self._target_field = target_field
        self._properties = properties

    @property
    def source(self) -> str:
        return self._source

    @property
    def type(self) -> str:
        return self._type

    @property
    def type_field(self) -> str:
        return self._type_field

    @property
    def source_field(self) -> str:
        return self._source_field

    @property
    def target_field(self) -> str:
        return self._target_field

    @property
    def properties(self) -> Dict[str, Any]:
        return self._properties

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source": self.source,
            "type": self._type,
            "type_field": self._type_field,
            "source_field": self._source_field,
            "target_field": self._target_field,
            "properties": self._properties,
        }

    def __str__(self):
        return str(self.to_dict())


class Graph:
    """
    -------------------------------
    A Graph model over source data.
    -------------------------------

    A Graph consists of:
      * A name
      * A db name (optional, default = "neo4j")
      * A List of Nodes (optional, but should have at least 1)
      * A List of Edges (optional, though boring if none!)
    """
    def __init__(self, *, name: str, db: str = "", nodes: List[Node] = [],
                 edges: List[Edge] = []):
        self.name = name
        self.db = db
        self.nodes = nodes
        self.edges = edges

    def named(self, name: str) -> 'Graph':
        return Graph(name=name, db=self.db, nodes=self.nodes, edges=self.edges)

    def in_db(self, db: str) -> 'Graph':
        return Graph(name=self.name, db=db, nodes=self.nodes, edges=self.edges)

    def with_nodes(self, nodes: List[Node]) -> 'Graph':
        return Graph(name=self.name, db=self.db, nodes=nodes, edges=self.edges)

    def with_edges(self, edges: List[Edge]) -> 'Graph':
        return Graph(name=self.name, db=self.db, nodes=self.nodes, edges=edges)

    def with_node(self, node: Node) -> 'Graph':
        return Graph(name=self.name, db=self.db, nodes=self.nodes + [node],
                     edges=self.edges)

    def with_edge(self, edge: Edge) -> 'Graph':
        return Graph(name=self.name, db=self.db, nodes=self.nodes,
                     edges=self.edges + [edge])

    def node_for_src(self, source: str) -> Union[None, Node]:
        """Find a Node in a Graph based on matching source pattern."""
        for node in self.nodes:
            if source.startswith(node.source):
                return node
        return None

    def edge_for_src(self, source: str) -> Union[None, Edge]:
        for edge in self.edges:
            if source.startswith(edge.source):
                return edge
        return None

    def edge_by_type(self, _type: str) -> Union[None, Edge]:
        for edge in self.edges:
            if edge.type == _type:
                return edge
        return None

    def node_by_label(self, label: str) -> Union[None, Node]:
        for node in self.nodes:
            if node.label == label:
                return node
        return None

    @classmethod
    def from_json(cls, json: str) -> 'Graph':
        obj = loads(json)
        nodes = [
            Node(source=n["source"], label=n.get("label", ""),
                 label_field=n["label_field"], key_field=n["key_field"],
                 **n.get("properties", {}))
            for n in obj.get("nodes", [])
        ]
        edges = [
            Edge(source=e["source"], edge_type=e.get("type", ""),
                 type_field=e["type_field"], source_field=e["source_field"],
                 target_field=e["target_field"], **e.get("properties", {}))
            for e in obj.get("edges", [])
        ]
        return Graph(name=obj["name"], db=obj.get("db", "neo4j"),
                     nodes=nodes, edges=edges)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "db": self.db,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
        }

    def to_json(self) -> str:
        return dumps(self, cls=_GraphEncoder)

    def __str__(self) -> str:
        return str(self.to_dict())

    def __eq__(self, other: Any) -> bool:
        if not other:
            return False
        if not isinstance(other, Graph):
            return False
        return self.to_dict() == other.to_dict()
