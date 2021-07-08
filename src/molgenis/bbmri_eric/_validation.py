import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, List, Optional

from molgenis.bbmri_eric._model import NodeData, Table
from molgenis.bbmri_eric.nodes import Node


class ValidationException(Exception):
    pass


id_spec_by_entity = {
    "persons": "contactID",
    "networks": "networkID",
    "biobanks": "ID",
    "collections": "ID",
}


@dataclass()
class ValidationState:

    invalid_ids: DefaultDict[str, List[ValidationException]] = field(
        default_factory=lambda: defaultdict(list)
    )
    invalid_references: DefaultDict[str, List[ValidationException]] = field(
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

    for table in node_data.tables:
        _validate_ids(table, node_data.node, state)

    _validate_networks(node_data.networks, state)
    _validate_biobanks(node_data.biobanks, state)
    _validate_collections(node_data.collections, state)

    return state


def _validate_ids(table: Table, node: Node, state: ValidationState):
    for row in table.rows:
        id_ = row["id"]
        errors = validate_bbmri_id(table, node, row["id"])
        if errors:
            state.invalid_ids[id_] += errors


def _validate_networks(networks: Table, state: ValidationState):
    for network in networks.rows:
        _validate_xref(network, "contact", state)
        _validate_mref(network, "parent_network", state)


def _validate_biobanks(biobanks: Table, state: ValidationState):
    for biobank in biobanks.rows:
        _validate_xref(biobank, "contact", state)
        _validate_mref(biobank, "network", state)


def _validate_collections(collections: Table, state: ValidationState):
    for collection in collections.rows:
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
            ValidationException(f"""{row["id"]} references invalid id: {ref_id}""")
        )


def validate_bbmri_id(
    table: Table, node: Node, bbmri_id: str
) -> Optional[List[ValidationException]]:
    errors = []
    # TODO refactor: split id on ':' and validate each piece separately

    id_spec = id_spec_by_entity[table.simple_name]

    id_constraint = f"bbmri-eric:{id_spec}:{node.code}_"  # for error messages
    global_id_constraint = f"bbmri-eric:{id_spec}:EU_"  # for global refs

    id_regex = f"^{id_constraint}"
    global_id_regex = f"^{global_id_constraint}"

    if not re.search(id_regex, bbmri_id) and not re.search(
        global_id_regex, bbmri_id
    ):  # they can ref to a global 'EU' entity.
        errors.append(
            ValidationException(
                f"""{bbmri_id} in entity: {table.full_name} does not start with
                {id_constraint} (or {global_id_constraint} if it's a xref/mref) """
            )
        )

    if re.search("[^A-Za-z0-9.@:_-]", bbmri_id):
        errors.append(
            ValidationException(
                f"""{bbmri_id} in entity: {table.full_name} contains characters other than:
                A-Z a-z 0-9 : _ -"""
            )
        )

    if re.search("::", bbmri_id):
        errors.append(
            ValidationException(
                f"""{bbmri_id} in entity: {table.full_name}
                contains :: indicating an empty component in ID hierarchy"""
            )
        )

    if not re.search("[A-Z]{2}_[A-Za-z0-9-_:@.]+$", bbmri_id):
        errors.append(
            ValidationException(
                f"""{bbmri_id} in entity: {table.full_name} does not comply with a
                two letter national node code, an _ and alphanumeric characters ( : @
                . are allowed) afterwards \ne.g: NL_myid1234 """
            )
        )

    return errors
