import re
from dataclasses import dataclass, field
from typing import List, Set

from molgenis.bbmri_eric._model import Node, NodeData, Table
from molgenis.bbmri_eric.errors import EricWarning


@dataclass()
class ValidationState:

    invalid_ids: Set[str] = field(default_factory=lambda: set())
    violations: List[EricWarning] = field(default_factory=lambda: list())

    def add_invalid_id(self, id_: str, violations: List[EricWarning]):
        self.invalid_ids.add(id_)
        self.violations.extend(violations)


def validate_node(node_data: NodeData) -> List[EricWarning]:
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

    return state.violations


def _validate_ids(table: Table, node: Node, state: ValidationState):
    for row in table.rows:
        id_ = row["id"]
        errors = validate_bbmri_id(table, node, row["id"])
        if errors:
            state.add_invalid_id(id_, errors)


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


def _validate_ref(row: dict, ref_id: str, state: ValidationState):
    if ref_id in state.invalid_ids:
        state.violations.append(
            EricWarning(f"{row['id']} references invalid id: {ref_id}")
        )


def validate_bbmri_id(table: Table, node: Node, id_: str) -> List[EricWarning]:
    errors = []

    prefix = node.get_id_prefix(table.type)
    if not id_.startswith(prefix):
        errors.append(
            EricWarning(
                f"{id_} in entity: {table.full_name} does not start with {prefix}"
            )
        )

    id_value = id_.lstrip(prefix)
    if not re.search("^[A-Za-z0-9-_:@.]+$", id_value):
        errors.append(
            EricWarning(
                f"Subpart {id_value} of {id_} in entity: {table.full_name} contains "
                f"invalid characters. Only alphanumerics and -_:@. are allowed."
            )
        )

    return errors
