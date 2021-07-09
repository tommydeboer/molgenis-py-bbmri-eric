from dataclasses import dataclass
from enum import Enum
from typing import List


class TableType(Enum):
    PERSONS = "persons"
    NETWORKS = "networks"
    BIOBANKS = "biobanks"
    COLLECTIONS = "collections"

    @classmethod
    def get_import_order(cls):
        return [type_.value for type_ in cls]


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

    def get_staging_id(self, table_type: TableType):
        return self.get_staging_id(table_type)

    def _get_staging_id(self, type_: TableType) -> str:
        return f"{self.package}_{self.code}_{type_.value}"

    def get_staging_table_ids(self) -> List[str]:
        return [
            self.persons_staging_id,
            self.networks_staging_id,
            self.biobanks_staging_id,
            self.collections_staging_id,
        ]


@dataclass(frozen=True)
class NodeData:
    # TODO rename because this might be confusing: is it external or staging data?
    # TODO introduce is_staging flag
    node: Node
    persons: Table
    networks: Table
    biobanks: Table
    collections: Table

    @property
    def tables(self):
        return [self.persons, self.networks, self.biobanks, self.collections]


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
