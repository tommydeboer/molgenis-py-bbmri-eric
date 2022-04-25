import typing
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


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
class TableMeta:
    """Convenient wrapper for the output of the metadata API."""

    meta: dict

    @property
    def id(self):
        return self.meta["data"]["id"]

    @property
    def id_attribute(self):
        for attribute in self.meta["data"]["attributes"]["items"]:
            if attribute["data"]["idAttribute"] is True:
                return attribute["data"]["name"]

    @property
    def one_to_manys(self) -> List[str]:
        one_to_manys = []
        for attribute in self.meta["data"]["attributes"]["items"]:
            if attribute["data"]["type"] == "onetomany":
                one_to_manys.append(attribute["data"]["name"])
        return one_to_manys

    @property
    def self_references(self) -> List[str]:
        self_references = []
        for attribute in self.meta["data"]["attributes"]["items"]:
            if attribute["data"]["type"] in ("xref", "mref"):
                if self.id in attribute["data"]["refEntityType"]["self"]:
                    self_references.append(attribute["data"]["name"])
        return self_references


@dataclass(frozen=True)
class Table:
    """
    Simple representation of a BBMRI ERIC node table. The rows should be in the
    uploadable format. (See _utils.py)
    """

    type: TableType
    rows_by_id: "typing.OrderedDict[str, dict]"
    meta: TableMeta = None

    @property
    def rows(self) -> List[dict]:
        return list(self.rows_by_id.values())

    @property
    def full_name(self) -> str:
        return self.meta.id

    @staticmethod
    def of(table_type: TableType, meta: TableMeta, rows: List[dict]) -> "Table":
        """Factory method that takes a list of rows instead of an OrderedDict of
        ids/rows."""
        rows_by_id = OrderedDict()
        for row in rows:
            rows_by_id[row["id"]] = row
        return Table(
            type=table_type,
            meta=meta,
            rows_by_id=rows_by_id,
        )

    @staticmethod
    def of_empty(table_type: TableType):
        return Table(table_type, OrderedDict())


@dataclass(frozen=True)
class Node:
    """Represents a single national node in the BBMRI ERIC directory."""

    code: str
    description: Optional[str]

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

    @classmethod
    def get_eu_id_prefix(cls, table_type: TableType) -> str:
        """
        Some nodes can refer to rows in the EU node. These rows have an EU prefix and
        it's based on the classifier of the table.

        :param TableType table_type: the table to get the EU id prefix for
        :return: the EU id prefix
        """

        classifier = cls._classifiers[table_type]
        return f"bbmri-eric:{classifier}:EU_"


@dataclass(frozen=True)
class ExternalServerNode(Node):
    """Represents a node that has an external server on which its data is hosted."""

    url: str


class Source(Enum):
    EXTERNAL_SERVER = "external_server"
    STAGING = "staging"
    PUBLISHED = "published"
    TRANSFORMED = "transformed"


# TODO introduce MixedData class


@dataclass()
class EricData:
    """Container object storing rows from the four ERIC tables. Can contain rows from
    multiple nodes."""

    source: Source
    persons: Table
    networks: Table
    biobanks: Table
    collections: Table
    table_by_type: Dict[TableType, Table] = field(init=False)

    def __post_init__(self):
        self.table_by_type = {
            TableType.PERSONS: self.persons,
            TableType.NETWORKS: self.networks,
            TableType.BIOBANKS: self.biobanks,
            TableType.COLLECTIONS: self.collections,
        }

    @property
    def import_order(self) -> List[Table]:
        return [self.persons, self.networks, self.biobanks, self.collections]

    @staticmethod
    def from_mixed_dict(source: Source, tables: Dict[TableType, Table]) -> "EricData":
        # TODO pass separate tables instead of dict?
        return EricData(
            source=source,
            persons=tables[TableType.PERSONS],
            networks=tables[TableType.NETWORKS],
            biobanks=tables[TableType.BIOBANKS],
            collections=tables[TableType.COLLECTIONS],
        )

    @staticmethod
    def from_empty(source: Source) -> "EricData":
        all_persons = Table.of_empty(TableType.PERSONS)
        all_networks = Table.of_empty(TableType.NETWORKS)
        all_biobanks = Table.of_empty(TableType.BIOBANKS)
        all_collections = Table.of_empty(TableType.COLLECTIONS)
        return EricData(
            source=source,
            persons=all_persons,
            networks=all_networks,
            biobanks=all_biobanks,
            collections=all_collections,
        )

    def merge(self, other_data: "EricData"):
        self.persons.rows_by_id.update(other_data.persons.rows_by_id)
        self.networks.rows_by_id.update(other_data.networks.rows_by_id)
        self.biobanks.rows_by_id.update(other_data.biobanks.rows_by_id)
        self.collections.rows_by_id.update(other_data.collections.rows_by_id)


@dataclass
class NodeData(EricData):
    """Container object storing the four tables of a single node."""

    node: Node

    @staticmethod
    def from_dict(
        node: Node, source: Source, tables: Dict[TableType, Table]
    ) -> "NodeData":
        return NodeData(
            node=node,
            source=source,
            persons=tables[TableType.PERSONS],
            networks=tables[TableType.NETWORKS],
            biobanks=tables[TableType.BIOBANKS],
            collections=tables[TableType.COLLECTIONS],
        )


@dataclass(frozen=True)
class QualityInfo:
    """
    Stores the quality information for biobanks and collections.
    """

    biobanks: Dict[str, List[str]]
    """Dictionary of biobank ids and their quality ids"""

    collections: Dict[str, List[str]]
    """Dictionary of collection ids and their quality ids"""

    def get_qualities(self, table_type: TableType) -> Dict[str, List[str]]:
        if table_type == TableType.BIOBANKS:
            return self.biobanks
        elif table_type == TableType.COLLECTIONS:
            return self.collections
        else:
            return dict()
