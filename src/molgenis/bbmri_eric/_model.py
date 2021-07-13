from dataclasses import dataclass
from enum import Enum
from typing import List


class TableType(Enum):
    PERSONS = "persons"
    NETWORKS = "networks"
    BIOBANKS = "biobanks"
    COLLECTIONS = "collections"

    @classmethod
    def get_import_order(cls) -> List["TableType"]:
        return [type_ for type_ in cls]

    @property
    def base_id(self) -> str:
        return f"eu_bbmri_eric_{self.value}"


@dataclass(frozen=True)
class Table:
    """
    Simple representation of a BBMRI ERIC node table. The rows should be in the
    uploadable format. (See _utils.py)
    """

    type: TableType
    full_name: str
    rows: List[dict]


@dataclass(frozen=True)
class Node:
    code: str
    package = "eu_bbmri_eric"

    def get_staging_id(self, table_type: TableType) -> str:
        return f"{self.package}_{self.code}_{table_type.value}"


@dataclass(frozen=True)
class ExternalServerNode(Node):
    url: str


@dataclass(frozen=True)
class NodeData:
    node: Node
    is_staging: bool
    persons: Table
    networks: Table
    biobanks: Table
    collections: Table

    @property
    def import_order(self) -> List[Table]:
        return [self.persons, self.networks, self.biobanks, self.collections]


_classifiers = {
    TableType.PERSONS: "contactID",
    TableType.NETWORKS: "networkID",
    TableType.BIOBANKS: "ID",
    TableType.COLLECTIONS: "ID",
}


def get_id_prefix(table_type: TableType, node: Node):
    classifier = _classifiers[table_type]
    return f"bbmri-eric:{classifier}:{node.code}_"
