from dataclasses import dataclass
from typing import List


@dataclass
class Node:
    code: str
    url: str = None


_nodes = {
    "AT": Node("AT"),
    "BE": Node("BE", "https://directory.bbmri.be"),
    "BG": Node("BG", "https://directory.bbmri.bg"),
    "CH": Node("CH"),
    "CY": Node("CY"),
    "CZ": Node("CZ"),
    "DE": Node("DE", "https://directory.bbmri.de"),
    "EE": Node("EE"),
    "EU": Node("EU"),
    "FI": Node("FI"),
    "FR": Node("FR"),
    "GR": Node("GR"),
    "IT": Node("IT"),
    "LV": Node("LV"),
    "MT": Node("MT"),
    "NL": Node("NL", "https://catalogue.bbmri.nl"),
    "NO": Node("NO"),
    "PL": Node("PL"),
    "SE": Node("SE"),
    "UK": Node("UK"),
    "EXT": Node("EXT"),
}


def get_node(code: str) -> Node:
    return _nodes[code]


def get_nodes(codes: List[str]) -> List[Node]:
    return [_nodes[code] for code in codes]


def get_all_nodes() -> List[Node]:
    return list(_nodes.values())
