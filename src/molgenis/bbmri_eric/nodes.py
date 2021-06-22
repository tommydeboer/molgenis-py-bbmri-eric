from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Node:
    code: str


@dataclass(frozen=True)
class ExternalNode(Node):
    url: str


_external_nodes = {
    "BE": ExternalNode("BE", "https://directory.bbmri.be"),
    "BG": ExternalNode("BG", "https://directory.bbmri.bg"),
    "DE": ExternalNode("DE", "https://directory.bbmri.de"),
    "NL": ExternalNode("NL", "https://catalogue.bbmri.nl"),
}

_nodes = {
    "AT": Node("AT"),
    "CH": Node("CH"),
    "CY": Node("CY"),
    "CZ": Node("CZ"),
    "EE": Node("EE"),
    "EU": Node("EU"),
    "FI": Node("FI"),
    "FR": Node("FR"),
    "GR": Node("GR"),
    "IT": Node("IT"),
    "LV": Node("LV"),
    "MT": Node("MT"),
    "NO": Node("NO"),
    "PL": Node("PL"),
    "SE": Node("SE"),
    "UK": Node("UK"),
    "EXT": Node("EXT"),
}

_nodes.update(_external_nodes)


def get_node(code: str) -> Node:
    return _nodes[code]


def get_nodes(codes: List[str]) -> List[Node]:
    return [_nodes[code] for code in codes]


def get_all_nodes() -> List[Node]:
    return list(_nodes.values())


def get_external_node(code: str) -> ExternalNode:
    return _external_nodes[code]


def get_external_nodes(codes: List[str]) -> List[ExternalNode]:
    return [_external_nodes[code] for code in codes]


def get_all_external_nodes() -> List[ExternalNode]:
    return list(_external_nodes.values())
