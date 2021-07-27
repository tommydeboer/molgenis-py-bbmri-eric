from typing import List

from molgenis.bbmri_eric._model import ExternalServerNode, Node

_external_nodes = {
    "BE": ExternalServerNode("BE", "https://directory.bbmri.be"),
    "BG": ExternalServerNode("BG", "https://bbmri-bg.molgeniscloud.org"),
    "DE": ExternalServerNode("DE", "https://directory.bbmri.de"),
    "NL": ExternalServerNode("NL", "https://catalogue.bbmri.nl"),
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
    return _nodes[code.upper()]


def get_nodes(codes: List[str]) -> List[Node]:
    return [_nodes[code.upper()] for code in codes]


def get_all_nodes() -> List[Node]:
    return list(_nodes.values())


def get_external_node(code: str) -> ExternalServerNode:
    return _external_nodes[code.upper()]


def get_external_nodes(codes: List[str]) -> List[ExternalServerNode]:
    return [_external_nodes[code.upper()] for code in codes]


def get_all_external_nodes() -> List[ExternalServerNode]:
    return list(_external_nodes.values())
