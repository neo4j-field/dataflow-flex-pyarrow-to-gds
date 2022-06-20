from .model import Graph, Node, Edge


def test_json_serde():
    """Test round-tripping a Graph through the JSON serialization methods."""
    g1 = (
        Graph("graph", "db")
        .with_node(Node("LabelA", "label", "key"))
        .with_node(Node("LabelB", "label", "key", prop1="prop1"))
        .with_edge(Edge("REL", "type", "src", "tgt", prop="prop"))
    )
    s = g1.to_json()
    g2 = Graph.from_json(s)
    assert g1 == g2
