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
    def __init__(self, label: str, label_field: str, key_field: str,
                 **properties: Dict[str, Any]):
        self._label = label
        self._label_field = label_field
        self._key_field = key_field
        self._properties = properties

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
            "label": self._label,
            "label_field": self._label_field,
            "key_field": self._key_field,
            "properties": self._properties,
        }

    def __str__(self) -> str:
        return str(self.to_dict())


class Edge:
    def __init__(self, edge_type: str, type_field: str, source_field: str,
                 target_field: str, **properties: Dict[str, Any]):
        self._type = edge_type
        self._type_field = type_field
        self._source_field = source_field
        self._target_field = target_field
        self._properties = properties

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
            "type": self._type,
            "type_field": self._type_field,
            "source_field": self._source_field,
            "target_field": self._target_field,
            "properties": self._properties,
        }

    def __str__(self):
        return str(self.to_dict())


class Graph:
    def __init__(self, name: str = "", db: str = "", nodes: List[Node] = [],
                 edges: List[Edge] = []):
        self.name = name
        self.db = db
        self.nodes = nodes
        self.edges = edges

    def named(self, name: str) -> 'Graph':
        return Graph(name, self.db, self.nodes, self.edges)

    def in_db(self, db: str) -> 'Graph':
        return Graph(self.name, db, self.nodes, self.edges)

    def with_nodes(self, nodes: List[Node]) -> 'Graph':
        return Graph(self.name, self.db, nodes, self.edges)

    def with_edges(self, edges: List[Edge]) -> 'Graph':
        return Graph(self.name, self.db, self.nodes, edges)

    def with_node(self, node: Node) -> 'Graph':
        return Graph(self.name, self.db, self.nodes + [node], self.edges)

    def with_edge(self, edge: Edge) -> 'Graph':
        return Graph(self.name, self.db, self.nodes, self.edges + [edge])

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
        g = Graph()
        obj = loads(json)
        nodes = [
            Node(n["label"], n["label_field"], n["key_field"],
                 **n["properties"])
            for n in obj.get("nodes", [])
        ]
        edges = [
            Edge(e["type"], e["type_field"], e["source_field"],
                 e["target_field"], **e["properties"])
            for e in obj.get("edges", [])
        ]
        return Graph(obj["name"], obj["db"], nodes, edges)

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
