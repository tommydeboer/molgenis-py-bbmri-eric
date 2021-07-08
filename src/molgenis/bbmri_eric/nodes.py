from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Node:
    code: str
    package = "eu_bbmri_eric"

    @property
    def persons_staging_id(self) -> str:
        return self._get_staging_id("persons")

    @property
    def networks_staging_id(self) -> str:
        return self._get_staging_id("networks")

    @property
    def biobanks_staging_id(self) -> str:
        return self._get_staging_id("biobanks")

    @property
    def collections_staging_id(self) -> str:
        return self._get_staging_id("collections")

    def _get_staging_id(self, simple_name: str) -> str:
        return f"eu_bbmri_eric_{self.code}_{simple_name}"

    def get_staging_table_ids(self) -> List[str]:
        return [
            self.persons_staging_id,
            self.networks_staging_id,
            self.biobanks_staging_id,
            self.collections_staging_id,
        ]


@dataclass(frozen=True)
class ExternalNode(Node):
    # TODO rename to IndependentNode
    url: str

    @property
    def persons_external_id(self) -> str:
        return self._get_id("persons")

    @property
    def networks_external_id(self) -> str:
        return self._get_id("networks")

    @property
    def biobanks_external_id(self) -> str:
        return self._get_id("biobanks")

    @property
    def collections_external_id(self) -> str:
        return self._get_id("collections")

    @staticmethod
    def _get_id(simple_name: str) -> str:
        return f"eu_bbmri_eric_{simple_name}"

    def get_external_table_ids(self) -> List[str]:
        return [
            self.persons_external_id,
            self.networks_external_id,
            self.biobanks_external_id,
            self.collections_external_id,
        ]


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
