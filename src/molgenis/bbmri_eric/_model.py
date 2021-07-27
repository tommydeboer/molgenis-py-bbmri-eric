from dataclasses import dataclass
from enum import Enum
from typing import Dict, List


class TableType(Enum):
    """Enum representing the four tables each national node has."""

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
    rows_by_id: Dict[str, dict]

    @property
    def rows(self) -> List[dict]:
        return list(self.rows_by_id.values())

    @staticmethod
    def of(table_type: TableType, full_name: str, rows: List[dict]) -> "Table":
        """Factory method that takes a list of rows instead of a dict of ids/rows."""
        return Table(
            type=table_type,
            full_name=full_name,
            rows_by_id={row["id"]: row for row in rows},
        )


@dataclass(frozen=True)
class Node:
    """Represents a single national node in the BBMRI ERIC directory."""

    code: str

    _classifiers = {
        TableType.PERSONS: "contactID",
        TableType.NETWORKS: "networkID",
        TableType.BIOBANKS: "ID",
        TableType.COLLECTIONS: "ID",
    }

    def get_staging_id(self, table_type: TableType) -> str:
        """
        Returns the identifier of a node's staging table.

        :param TableType table_type: the table to get the staging id of
        :return: the id of the staging table
        """
        return f"eu_bbmri_eric_{self.code}_{table_type.value}"

    def get_id_prefix(self, table_type: TableType) -> str:
        """
        Each table has a specific prefix for the identifiers of its rows. This prefix is
        based on the node's code and the classifier of the table.

        :param TableType table_type: the table to get the id prefix for
        :return: the id prefix
        """
        classifier = self._classifiers[table_type]
        return f"bbmri-eric:{classifier}:{self.code}_"


@dataclass(frozen=True)
class ExternalServerNode(Node):
    """Represents a node that has an external server on which its data is hosted."""

    url: str


@dataclass(frozen=True)
class NodeData:
    """Container object storing the four tables of a single node."""

    node: Node
    is_staging: bool
    persons: Table
    networks: Table
    biobanks: Table
    collections: Table

    @property
    def import_order(self) -> List[Table]:
        return [self.persons, self.networks, self.biobanks, self.collections]
