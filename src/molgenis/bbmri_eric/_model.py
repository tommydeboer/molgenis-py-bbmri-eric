from dataclasses import dataclass
from typing import List

from molgenis.bbmri_eric.nodes import Node


@dataclass(frozen=True)
class Table:
    simple_name: str
    full_name: str
    rows: List[dict]


@dataclass(frozen=True)
class NodeData:
    # TODO rename because this might be confusing: is it external or staging data?

    node: Node
    persons: Table
    networks: Table
    biobanks: Table
    collections: Table

    @property
    def tables(self):
        return [self.persons, self.networks, self.biobanks, self.collections]
