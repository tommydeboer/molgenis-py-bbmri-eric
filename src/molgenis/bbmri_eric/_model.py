from dataclasses import dataclass
from enum import Enum
from typing import List

from molgenis.bbmri_eric.nodes import Node


class TableType(Enum):
    PERSONS = "persons"
    NETWORKS = "networks"
    BIOBANKS = "biobanks"
    COLLECTIONS = "collections"


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
