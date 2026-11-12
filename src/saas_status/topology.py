"""Versioned service topology model and graph engine.

A topology document describes one *generation*.  Nodes (services/components)
carry stable identifiers plus a generation-scoped display name; edges are
``depends_on`` (service -> component, component -> component is also allowed)
or ``owns`` (service -> component) relations.  Generations are immutable once
stored: renaming a node in a new generation never rewrites historical rows.
"""
from __future__ import annotations

from dataclasses import dataclass, field

VALID_NODE_KINDS = ("service", "component")
VALID_EDGE_KINDS = ("depends_on", "owns")

# Reason codes surfaced on undetermined results.
REASON_NO_TOPOLOGY = "no_topology"
REASON_UNRESOLVED_NAME = "unresolved_name"
REASON_DANGLING_REFERENCE = "dangling_reference"
REASON_CYCLE_DEPENDENCY = "cycle_dependency"
REASON_UNKNOWN_STATE = "unknown_state"

# Guards for path enumeration on pathological graphs.
MAX_PATH_DEPTH = 32
MAX_PATHS_PER_INCIDENT = 64


@dataclass(frozen=True)
class NodeVersion:
    node_id: str
    kind: str
    name: str
    generation_id: str
    effective_from: str
    effective_until: str | None = None


@dataclass(frozen=True)
class EdgeVersion:
    edge_id: str
    kind: str
    source_id: str
    target_id: str
    generation_id: str
    effective_from: str
    effective_until: str | None = None


@dataclass(frozen=True)
class TopologySnapshot:
    """Immutable materialized view of every node/edge effective at one instant."""

    as_of: str
    generation_id: str | None
    nodes: dict[str, NodeVersion] = field(default_factory=dict)
    edges: list[EdgeVersion] = field(default_factory=list)
    # node_id -> every (generation_id, name, kind) version ever stored; lets the
    # graph resolve an event's historical component name across renames.
    node_aliases: dict[str, tuple[NodeVersion, ...]] = field(default_factory=dict)

    def name_of(self, node_id: str) -> str | None:
        node = self.nodes.get(node_id)
        return node.name if node else None

    def kind_of(self, node_id: str) -> str | None:
        node = self.nodes.get(node_id)
        return node.kind if node else None


@dataclass(frozen=True)
class TopologyDocument:
    generation_id: str
    effective_from: str
    effective_until: str | None
    nodes: list[NodeVersion]
    edges: list[EdgeVersion]
    raw_hash: str

    @staticmethod
    def from_raw(raw: dict, raw_hash: str) -> "TopologyDocument":
        if not isinstance(raw, dict):
            raise ValueError("topology document must be an object")
        generation_id = str(raw["generation_id"]).strip()
        effective_from = str(raw["effective_from"]).strip()
        effective_until = raw.get("effective_until")
        if effective_until is not None:
            effective_until = str(effective_until).strip() or None
        if not generation_id or not effective_from:
            raise ValueError("generation_id and effective_from are required")

        nodes: list[NodeVersion] = []
        seen_node_ids: set[str] = set()
        for item in raw.get("nodes", []):
            node_id = str(item["id"]).strip()
            kind = str(item["kind"]).strip().lower()
            name = " ".join(str(item.get("name", node_id)).split())
            if kind not in VALID_NODE_KINDS:
                raise ValueError(f"node {node_id!r} has invalid kind {kind!r}")
            if node_id in seen_node_ids:
                raise ValueError(f"duplicate node id {node_id!r} in generation {generation_id!r}")
            seen_node_ids.add(node_id)
            nodes.append(NodeVersion(node_id, kind, name, generation_id, effective_from, effective_until))

        edges: list[EdgeVersion] = []
        seen_edge_ids: set[str] = set()
        for item in raw.get("edges", []):
            kind = str(item["kind"]).strip().lower()
            if kind not in VALID_EDGE_KINDS:
                raise ValueError(f"edge has invalid kind {kind!r}")
            source_id = str(item["source"]).strip()
            target_id = str(item["target"]).strip()
            edge_id = str(item.get("id", "")).strip() or f"{kind}:{source_id}->{target_id}"
            if edge_id in seen_edge_ids:
                raise ValueError(f"duplicate edge id {edge_id!r} in generation {generation_id!r}")
            seen_edge_ids.add(edge_id)
            edges.append(
                EdgeVersion(edge_id, kind, source_id, target_id, generation_id, effective_from, effective_until)
            )
        return TopologyDocument(generation_id, effective_from, effective_until, nodes, edges, raw_hash)


@dataclass(frozen=True)
class NameResolution:
    node_ids: frozenset[str]
    reason: str | None = None
    detail: str | None = None


class TopologyGraph:
    """Graph operations over a single :class:`TopologySnapshot`.

    The impact graph follows ``depends_on`` edges backwards (component -> the
    services that depend on it).  ``owns`` edges map components to their owner
    services and explain attribution when no dependency edge exists.
    """

    def __init__(self, snapshot: TopologySnapshot) -> None:
        self.snapshot = snapshot
        self._depends_reverse: dict[str, list[str]] = {}
        self._depends_forward: dict[str, list[str]] = {}
        self._owners: dict[str, list[str]] = {}
        self.dangling_edges: list[EdgeVersion] = []
        for edge in snapshot.edges:
            if edge.source_id not in snapshot.nodes or edge.target_id not in snapshot.nodes:
                self.dangling_edges.append(edge)
                continue
            if edge.kind == "depends_on":
                self._depends_forward.setdefault(edge.source_id, []).append(edge.target_id)
                self._depends_reverse.setdefault(edge.target_id, []).append(edge.source_id)
            elif edge.kind == "owns":
                self._owners.setdefault(edge.target_id, []).append(edge.source_id)

    # ------------------------------------------------------------------ names
    def resolve_name(self, name: str, kind: str) -> NameResolution:
        """Resolve a display name to a stable node id.

        The current generation is tried first.  When the name only existed in an
        older generation (the node was renamed), its stable id is returned via
        the cross-generation alias table so historical events stay attached to
        the component instead of becoming unresolvable.  Ambiguity (one name on
        several ids, including across generations) stays undetermined.
        """
        wanted = name.strip().lower()
        current_matches = frozenset(
            node.node_id
            for node in self.snapshot.nodes.values()
            if node.kind == kind and node.name.strip().lower() == wanted
        )
        if len(current_matches) == 1:
            return NameResolution(current_matches)
        if len(current_matches) > 1:
            return NameResolution(
                current_matches,
                REASON_UNRESOLVED_NAME,
                f"name {name!r} is ambiguous in generation {self.snapshot.generation_id!r}: "
                f"matches {sorted(current_matches)}",
            )

        alias_matches: set[str] = set()
        alias_generations: dict[str, list[str]] = {}
        for node_id, versions in self.snapshot.node_aliases.items():
            for version in versions:
                if version.kind == kind and version.name.strip().lower() == wanted:
                    alias_matches.add(node_id)
                    alias_generations.setdefault(node_id, []).append(version.generation_id)
        if not alias_matches:
            return NameResolution(
                frozenset(),
                REASON_UNRESOLVED_NAME,
                f"no {kind} node named {name!r} across any stored topology generation",
            )
        if len(alias_matches) > 1:
            return NameResolution(
                frozenset(alias_matches),
                REASON_UNRESOLVED_NAME,
                f"name {name!r} matches multiple stable ids across generations: {sorted(alias_matches)}",
            )
        return NameResolution(frozenset(alias_matches))

    # ----------------------------------------------------------------- cycles
    def cyclic_components(self) -> list[list[str]]:
        """Return strongly connected components that contain a dependency cycle."""
        index_counter = [0]
        stack: list[str] = []
        on_stack: set[str] = set()
        indices: dict[str, int] = {}
        lowlink: dict[str, int] = {}
        result: list[list[str]] = []
        neighbors = self._depends_forward

        def strong_connect(node: str) -> None:
            indices[node] = index_counter[0]
            lowlink[node] = index_counter[0]
            index_counter[0] += 1
            stack.append(node)
            on_stack.add(node)
            for neighbour in neighbors.get(node, ()):  # dangling targets already excluded
                if neighbour not in indices:
                    strong_connect(neighbour)
                    lowlink[node] = min(lowlink[node], lowlink[neighbour])
                elif neighbour in on_stack:
                    lowlink[node] = min(lowlink[node], indices[neighbour])
            if lowlink[node] == indices[node]:
                component: list[str] = []
                while True:
                    member = stack.pop()
                    on_stack.discard(member)
                    component.append(member)
                    if member == node:
                        break
                cyclic = len(component) > 1 or node in neighbors.get(node, ())
                if cyclic:
                    result.append(sorted(component))

        for node in self.snapshot.nodes:
            if node not in indices:
                strong_connect(node)
        return result

    def services_touching_cycles(self) -> dict[str, list[list[str]]]:
        """Map service id -> the cyclic component id lists it depends on."""
        cycles = self.cyclic_components()
        if not cycles:
            return {}
        affected: dict[str, list[list[str]]] = {}
        for cycle in cycles:
            reachable_services: set[str] = set()
            frontier = list(cycle)
            seen: set[str] = set(frontier)
            while frontier:
                current = frontier.pop()
                for dependent in self._depends_reverse.get(current, ()):
                    if dependent in seen:
                        continue
                    seen.add(dependent)
                    if self.snapshot.kind_of(dependent) == "service":
                        reachable_services.add(dependent)
                    frontier.append(dependent)
            for service_id in reachable_services:
                affected.setdefault(service_id, []).append(cycle)
        return affected

    # --------------------------------------------------------------- impact
    def affected_services(self, component_id: str) -> set[str]:
        """Services impacted by an incident on ``component_id``.

        Combines reverse ``depends_on`` reachability with direct ownership.
        """
        affected: set[str] = set()
        affected.update(self._owners.get(component_id, ()))
        seen = {component_id}
        frontier = [component_id]
        while frontier:
            current = frontier.pop()
            for dependent in self._depends_reverse.get(current, ()):
                if dependent in seen:
                    continue
                seen.add(dependent)
                if self.snapshot.kind_of(dependent) == "service":
                    affected.add(dependent)
                frontier.append(dependent)
        return affected

    def impact_paths(self, component_id: str, service_id: str) -> list[list[str]]:
        """Simple component -> service paths, annotated with the edge kind."""
        paths: list[list[str]] = []

        def walk(node: str, trail: list[str], edge_kinds: list[str]) -> None:
            if len(paths) >= MAX_PATHS_PER_INCIDENT or len(trail) > MAX_PATH_DEPTH:
                return
            if node == service_id:
                paths.append(trail)
                return
            for dependent in self._depends_reverse.get(node, ()):
                if dependent in trail:
                    continue
                walk(dependent, trail + [dependent], edge_kinds + ["depends_on"])
            # Ownership is a terminal attribution, only tried from the root.
            if node == component_id:
                for owner in self._owners.get(component_id, ()):
                    if owner == service_id and [component_id, owner] not in paths:
                        paths.append([component_id, owner])

        walk(component_id, [component_id], [])
        return paths

    def dangling_reasons_for_service(self, service_id: str) -> list[dict]:
        reasons = []
        for edge in self.dangling_edges:
            if edge.source_id == service_id or service_id in self._reachable_dependents(edge.source_id):
                target_name = self.snapshot.name_of(edge.target_id) or edge.target_id
                reasons.append(
                    {
                        "code": REASON_DANGLING_REFERENCE,
                        "detail": (
                            f"edge {edge.edge_id!r} ({edge.kind}) from {edge.source_id!r} "
                            f"references missing node {edge.target_id!r} (name {target_name!r})"
                        ),
                        "edge_id": edge.edge_id,
                        "missing_node_id": edge.target_id,
                    }
                )
        return reasons

    def _reachable_dependents(self, start_id: str) -> set[str]:
        seen = {start_id}
        frontier = [start_id]
        while frontier:
            current = frontier.pop()
            for dependent in self._depends_reverse.get(current, ()):
                if dependent not in seen:
                    seen.add(dependent)
                    frontier.append(dependent)
        return seen
