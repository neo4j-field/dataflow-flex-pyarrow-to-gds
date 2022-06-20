from enum import Enum
import logging as log
import json

import pyarrow as pa
import pyarrow.flight as flight

from .model import Graph, Node, Edge

from typing import Any, Dict, Iterable, Optional, Union, Tuple


Result = Tuple[int, int]
Arrow = Union[pa.Table, pa.RecordBatch]
Nodes = Union[pa.Table, Iterable[pa.RecordBatch]]
Edges = Union[pa.Table, Iterable[pa.RecordBatch]]


class ClientState(Enum):
    READY = "ready"
    FEEDING_NODES = "feeding_nodes"
    FEEDING_EDGES = "feeding_edges"
    AWAITING_GRAPH = "awaiting_graph"
    GRAPH_READY = "done"


class Neo4jArrowClient:
    def __init__(self, host: str, graph: str, *, port: int=8491, debug = False,
                 user: str = "neo4j", password: str = "neo4j", tls: bool = True,
                 concurrency: int = 4, database: str = "neo4j"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls = tls
        self.client: flight.FlightClient = None
        self.call_opts = None
        self.graph = graph
        self.database = database
        self.concurrency = concurrency
        self.state = ClientState.READY
        self.debug = debug

    def __str__(self):
        return f"Neo4jArrowClient{{{self.user}@{self.host}:{self.port}" \
            f"/{self.graph}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "client" in state:
            del state["client"]
        if "call_opts" in state:
            del state["call_opts"]
        return state

    def copy(self) -> 'Neo4jArrowClient':
        client = Neo4jArrowClient(self.host, port=self.port, user=self.user,
                                  password=self.password, graph=self.graph,
                                  tls=self.tls, concurrency=self.concurrency,
                                  database=self.database)
        client.state = self.state
        return client

    def _client(self) -> flight.FlightClient:
        """Lazy client construction to help pickle this class."""
        if not hasattr(self, "client") or not self.client:
            self.call_opts = None
            if self.tls:
                location = flight.Location.for_grpc_tls(self.host, self.port)
            else:
                location = flight.Location.for_grpc_tcp(self.host, self.port)
            client = flight.FlightClient(location)
            if self.user and self.password:
                (header, token) = client.authenticate_basic_token(self.user,
                                                                  self.password)
                if header:
                    self.call_opts = flight.FlightCallOptions(
                        headers=[(header, token)]
                    )
            self.client = client
        return self.client

    def _send_action(self, action: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """
        Communicates an Arrow Action message to the GDS Arrow Service.
        """
        client = self._client()
        try:
            payload = json.dumps(body).encode("utf-8")
            result = client.do_action(
                flight.Action(action, payload),
                options=self.call_opts
            )
            obj = json.loads(next(result).body.to_pybytes().decode())
            return dict(obj)
        except Exception as e:
            log.error(f"send_action error: {e}")
            raise e

    @classmethod
    def _nop(*args, **kwargs):
        """Used as a no-op mapping function."""
        pass

    @classmethod
    def _node_mapper(cls, model: Graph):
        def _map(node: Arrow) -> Arrow:
            schema = node.schema
            my_label = node["labels"][0].as_py() # TODO: search based on lbls field
            n = model.node_by_label(my_label)
            if not n:
                raise Exception(f"cannot find node for label '{my_label}'")
            for idx, name in enumerate(node.schema.names):
                field = schema.field(name)
                if name in n.key_field:
                    schema = schema.set(idx, field.with_name("nodeId"))
                elif name in n.label_field:
                    schema = schema.set(idx, field.with_name("labels"))
                #todo: labels, props, etc.
            return node.from_arrays(node.columns, schema=schema)
        return _map

    @classmethod
    def _edge_mapper(cls, model: Graph):
        def _map(edge: Arrow) -> Arrow:
            schema = edge.schema
            my_type = edge["type"][0].as_py() # TODO: search based on type fields
            e = model.edge_by_type(my_type)
            if not e:
                raise Exception(f"cannot find edge for type '{my_type}'")
            for idx, name in enumerate(edge.schema.names):
                f = schema.field(name)
                if name == e.source_field:
                    schema = schema.set(idx, f.with_name("sourceNodeId"))
                elif name == e.target_field:
                    schema = schema.set(idx, f.with_name("targetNodeId"))
                elif name == e.type_field:
                    schema = schema.set(idx, f.with_name("relationshipType"))
            return edge.from_arrays(edge.columns, schema=schema)
        return _map

    def _write_table(self, desc: Dict[str, Any], table: pa.Table,
                     mappingfn = None) -> Result:
        """
        Write a PyArrow Table to the GDS Flight service.
        """
        fn = mappingfn or self._nop
        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        table = fn(table)
        writer, _ = client.do_put(upload_descriptor, table.schema,
                                  options=self.call_opts)
        with writer:
            try:
                writer.write_table(table)
                return table.num_rows, table.get_total_buffer_size()
            except Exception as e:
                log.error(f"_write_table error: {e}")
        return 0, 0

    def _write_batches(self, desc: Dict[str, Any], batches,
                       mappingfn = None) -> Result:
        """
        Write PyArrow RecordBatches to the GDS Flight service.
        """
        batches = iter(batches)
        fn = mappingfn or self._nop

        first = fn(next(batches, None)) # type: ignore
        if not first:
            raise Exception("empty iterable of record batches provided")

        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        rows, nbytes = 0, 0
        writer, _ = client.do_put(upload_descriptor, first.schema,
                                  options=self.call_opts)
        with writer:
            try:
                writer.write_batch(first)
                rows += first.num_rows
                nbytes += first.get_total_buffer_size()
                for remaining in batches:
                    writer.write_batch(fn(remaining))
                    rows += remaining.num_rows
                    nbytes += remaining.get_total_buffer_size()
            except Exception as e:
                log.error(f"_write_batches error: {e}")
        return rows, nbytes

    def start(self, action: str = "CREATE_GRAPH", *,
              config: Dict[str, Any] = {}) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.READY
        if not config:
            config = {
                "name": self.graph,
                "database_name": self.database,
                "concurrency": self.concurrency,
            }
        result = self._send_action(action, config)
        if result:
            self.state = ClientState.FEEDING_NODES
        return result

    def write_nodes(self, nodes: Nodes,
                    model: Optional[Graph] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_NODES
        desc = { "name": self.graph, "entity_type": "node" }
        if model:
            mapper = self._node_mapper(model)
        else:
            mapper = self._nop
        if isinstance(nodes, pa.Table):
            return self._write_table(desc, nodes, mapper)
        return self._write_batches(desc, nodes, mapper)

    def nodes_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_NODES
        result = self._send_action("NODE_LOAD_DONE", { "name": self.graph })
        if result:
            self.state = ClientState.FEEDING_EDGES
        return result

    def write_edges(self, edges: Edges,
                    model: Optional[Graph] = None) -> Result:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES
        desc = { "name": self.graph, "entity_type": "relationship" }
        if model:
            mapper = self._edge_mapper(model)
        else:
            mapper = self._nop
        if isinstance(edges, pa.Table):
            return self._write_table(desc, edges, mapper)
        return self._write_batches(desc, edges, mapper)

    def edges_done(self) -> Dict[str, Any]:
        assert not self.debug or self.state == ClientState.FEEDING_EDGES
        result = self._send_action("RELATIONSHIP_LOAD_DONE",
                                   { "name": self.graph })
        if result:
            self.state = ClientState.AWAITING_GRAPH
        return result

    def read_edges(self, prop: str, *, concurrency: int = 4):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamRelationshipProperty",
            "configuration": {
                "relationship_types": "*",
                "relationship_property": prop,
            },
            "concurrency": concurrency,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk

    def read_nodes(self, prop: str, *, concurrency: int = 4):
        ticket = {
            "graph_name": self.graph, "database_name": self.database,
            "procedure_name": "gds.graph.streamNodeProperty",
            "configuration": {
                "node_labels": "*",
                "node_property": prop,
            },
            "concurrency": concurrency,
        }

        client = self._client()
        result = client.do_get(
            pa.flight.Ticket(json.dumps(ticket).encode("utf8")),
            options=self.call_opts
        )
        for chunk, _ in result:
            yield chunk


    def wait(self, timeout: int = 0):
        """wait for completion"""
        assert not self.debug or self.state == ClientState.AWAITING_GRAPH
        self.state = ClientState.AWAITING_GRAPH
        # TODO: return future? what do we do?
        pass
