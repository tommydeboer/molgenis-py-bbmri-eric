import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List, Optional

from molgenis.bbmri_eric._model import Node, NodeData, Table, TableType


@dataclass(frozen=True)
class ConstraintViolation:
    message: str


_classifiers = {
    TableType.PERSONS: "contactID",
    TableType.NETWORKS: "networkID",
    TableType.BIOBANKS: "ID",
    TableType.COLLECTIONS: "ID",
}


@dataclass()
class ValidationState:

    invalid_ids: DefaultDict[str, List[ConstraintViolation]] = field(
        default_factory=lambda: defaultdict(list)
    )
    invalid_references: DefaultDict[str, List[ConstraintViolation]] = field(
        default_factory=lambda: defaultdict(list)
    )

    @property
    def errors(self):
        return sum(self.invalid_ids.values(), []) + sum(
            self.invalid_references.values(), []
        )


def validate_node(node_data: NodeData) -> ValidationState:
    """
    Validates the staging tables of a single node. Keeps track of any invalid rows in a
    ValidationState object.
    """
    state = ValidationState()

    for table in node_data.import_order:
        _validate_ids(table, node_data.node, state)

    _validate_networks(node_data, state)
    _validate_biobanks(node_data, state)
    _validate_collections(node_data, state)

    return state


def _validate_ids(table: Table, node: Node, state: ValidationState):
    for row in table.rows:
        id_ = row["id"]
        errors = validate_bbmri_id(table, node, row["id"])
        if errors:
            state.invalid_ids[id_] += errors


def _validate_networks(node_data: NodeData, state: ValidationState):
    for network in node_data.networks.rows:
        _validate_xref(network, "contact", state)
        _validate_mref(network, "parent_network", state)


def _validate_biobanks(node_data: NodeData, state: ValidationState):
    for biobank in node_data.biobanks.rows:
        _validate_xref(biobank, "contact", state)
        _validate_mref(biobank, "network", state)


def _validate_collections(node_data: NodeData, state: ValidationState):
    for collection in node_data.collections.rows:
        _validate_xref(collection, "contact", state)
        _validate_xref(collection, "biobank", state)
        _validate_mref(collection, "parent_collection", state)
        _validate_mref(collection, "networks", state)


def _validate_xref(row: dict, ref_attr: str, state: ValidationState):
    if ref_attr in row:
        _validate_ref(row, row[ref_attr], state)


def _validate_mref(row: dict, mref_attr: str, state: ValidationState):
    if mref_attr in row:
        for ref_id in row[mref_attr]:
            _validate_ref(row, ref_id, state)


def _validate_ref(row: dict, ref_id: str, state):
    if ref_id in state.invalid_ids:
        state.invalid_references[ref_id].append(
            ConstraintViolation(f"{row['id']} references invalid id: {ref_id}")
        )


def validate_bbmri_id(
    table: Table, node: Node, id_: str
) -> Optional[List[ConstraintViolation]]:
    errors = []
    classifier = _classifiers[table.type]
    prefix = f"bbmri-eric:{classifier}:{node.code}_"

    if not id_.startswith(prefix):
        errors.append(
            ConstraintViolation(
                f"{id_} in entity: {table.full_name} does not start with {prefix}"
            )
        )

    id_value = id_.lstrip(prefix)
    if not re.search("^[A-Za-z0-9-_:@.]+$", id_value):
        errors.append(
            ConstraintViolation(
                f"Subpart {id_value} of {id_} in entity: {table.full_name} contains "
                f"invalid characters. Only alphanumerics and -_:@. are allowed."
            )
        )

    return errors
