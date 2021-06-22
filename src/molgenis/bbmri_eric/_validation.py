import re
from typing import List

from molgenis.bbmri_eric.nodes import Node

id_spec_by_entity = {
    "persons": "contactID",
    "contact": "contactID",
    "networks": "networkID",
    "biobanks": "ID",
    "collections": "ID",  # collectionID
    "sub_collections": "ID",  # ref-check
}


def validate_bbmri_id(entity, node: Node, bbmri_id):
    errors = []

    if entity not in id_spec_by_entity:
        return True  # no constraints found

    id_spec = id_spec_by_entity[entity]

    id_constraint = f"bbmri-eric:{id_spec}:{node.code}_"  # for error messages
    global_id_constraint = f"bbmri-eric:{id_spec}:EU_"  # for global refs

    id_regex = f"^{id_constraint}"
    global_id_regex = f"^{global_id_constraint}"

    if not re.search(id_regex, bbmri_id) and not re.search(
        global_id_regex, bbmri_id
    ):  # they can ref to a global 'EU' entity.
        errors.append(
            f"""{bbmri_id} in entity: {entity} does not start with {id_constraint} (or
            {global_id_constraint} if it's a xref/mref)"""
        )

    if re.search("[^A-Za-z0-9.@:_-]", bbmri_id):
        errors.append(
            f"""{bbmri_id} in entity: {entity} contains characters other than:
            A-Z a-z 0-9 : _ -"""
        )

    if re.search("::", bbmri_id):
        errors.append(
            f"""{bbmri_id} in entity: {entity}
            contains :: indicating an empty component in ID hierarchy"""
        )

    if not re.search("[A-Z]{2}_[A-Za-z0-9-_:@.]+$", bbmri_id):
        errors.append(
            f"""{bbmri_id} in entity: {entity} does not comply with a two letter
            national node code, an _ and alphanumeric characters ( : @ . are allowed)
            afterwards \ne.g: NL_myid1234"""
        )

    for error in errors:
        print(error)

    return len(errors) == 0


def _validate_id_in_nn_entry(
    entity: str, parent_entry: dict, parent_entity: str, node: Node, entry: dict
) -> bool:
    ref_bbmri_id = entry["id"]
    parent_id = parent_entry["id"]

    if not validate_bbmri_id(entity=entity, node=node, bbmri_id=ref_bbmri_id):
        print(
            f"""{parent_id} in entity: {parent_entity} contains references to
            entity: {entity} with an invalid id ({ref_bbmri_id})"""
        )
        return False
    else:
        return True


# get all ref ids and then check
def validate_refs_in_entry(
    node: Node,
    entry: dict,
    parent_entity: str,
    possible_entity_references: List[str],
) -> List[str]:

    validations = []

    for entity_reference in possible_entity_references:
        if entity_reference not in entry or entity_reference not in id_spec_by_entity:
            continue

        ref_data = entry[entity_reference]

        # check if its an xref
        if type(ref_data) is dict:
            valid_id = _validate_id_in_nn_entry(
                entity=entity_reference,
                parent_entry=entry,
                parent_entity=parent_entity,
                node=node,
                entry=ref_data,
            )
            validations.append(
                {
                    "entity_reference": entity_reference,
                    "ref_id": ref_data["id"],
                    "valid": valid_id,
                }
            )
        else:
            for ref in ref_data:
                if type(ref) is dict:
                    valid_id = _validate_id_in_nn_entry(
                        entity=entity_reference,
                        parent_entry=entry,
                        parent_entity=parent_entity,
                        node=node,
                        entry=ref,
                    )
                    validations.append(
                        {
                            "entity_reference": entity_reference,
                            "ref_id": ref["id"],
                            "valid": valid_id,
                        }
                    )
                else:
                    if not validate_bbmri_id(
                        entity=entity_reference, node=node, bbmri_id=ref
                    ):
                        validations.append(
                            {
                                "entity_reference": entity_reference,
                                "ref_id": ref,
                                "valid": False,
                            }
                        )
    return validations
