from .model import Graph, Node, Edge


def test_json_serde():
    """Test round-tripping a Graph through the JSON serialization methods."""
    g1 = (
        Graph(name="graph", db="db")
        .with_node(Node(source="a", label_field="label", key_field="key"))
        .with_node(Node(source="b", label="LabelB", label_field="label",
                        key_field="key", prop1="prop1"))
        .with_edge(Edge(source="r", edge_type="REL", type_field="type",
                        source_field="src", target_field="tgt", prop="prop"))
    )
    s = g1.to_json()
    g2 = Graph.from_json(s)
    print(f"g1: {g1}")
    print(f"g2: {g2}")
    assert g1 == g2


def test_reading_model_from_json():
    json = """
{
    "name": "faux-graph",
    "db": "neo4j",
    "nodes": [
        { "source": "papers", "label_field": "labels", "key_field": "paper" },
        { "source": "authors", "label_field": "labels", "key_field": "author" }
    ],
    "edges": [
        { "source": "citations", "type_field": "type", "source_field": "source",
          "target_field": "target" },
        { "source": "authorship", "type_field": "type",
          "source_field": "author", "target_field": "paper" }
    ]
}
    """
    g = Graph.from_json(json)
    assert g.name == "faux-graph"
    assert g.db == "neo4j"
    assert len(g.nodes) == 2
    assert len(g.edges) == 2
    assert g.nodes[0].source == "papers"
