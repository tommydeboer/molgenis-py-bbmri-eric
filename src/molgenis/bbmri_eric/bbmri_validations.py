import re

registered_national_nodes = [
    "AT",
    "BE",
    "BG",
    "CH",
    "CY",
    "CZ",
    "DE",
    "EE",
    "EU",
    "FI",
    "FR",
    "GR",
    "IT",
    "LV",
    "MT",
    "NL",
    "NO",
    "PL",
    "SE",
    "UK",
    "EXT",
]

idSpecByEntity = {
    "persons": "contactID",
    "contact": "contactID",
    "networks": "networkID",
    "biobanks": "ID",
    "collections": "ID",  # collectionID
    "sub_collections": "ID",  # ref-check
}


def validate_bbmri_id(entity, nn, bbmri_id):
    errors = []

    if entity not in idSpecByEntity:
        return True  # no constraints found

    idSpec = idSpecByEntity[entity]

    idConstraint = f"bbmri-eric:{idSpec}:{nn}_"  # for error messages
    globalIdConstraint = f"bbmri-eric:{idSpec}:EU_"  # for global refs

    idRegex = f"^{idConstraint}"
    globalIdRegex = f"^{globalIdConstraint}"

    if not re.search(idRegex, bbmri_id) and not re.search(
        globalIdRegex, bbmri_id
    ):  # they can ref to a global 'EU' entity.
        errors.append(
            f"""{bbmri_id} in entity: {entity} does not start with {idConstraint} (or
            {globalIdConstraint} if it's a xref/mref)"""
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
    entity: str, parent_entry: dict, parent_entity: str, nn: dict, entry: dict
) -> bool:
    ref_bbmri_id = entry["id"]
    parent_id = parent_entry["id"]

    if not validate_bbmri_id(entity=entity, nn=nn, bbmri_id=ref_bbmri_id):
        print(
            f"""{parent_id} in entity: {parent_entity} contains references to
            entity: {entity} with an invalid id ({ref_bbmri_id})"""
        )
        return False
    else:
        return True


# get all ref ids and then check
def validate_refs_in_entry(
    nn: dict,
    entry: dict,
    parent_entity: str,
    possible_entity_references: list,
    valid_entities: list[str] = None,
) -> list[dict]:

    validations = []

    for entity_reference in possible_entity_references:
        if entity_reference not in entry or entity_reference not in idSpecByEntity:
            continue

        ref_data = entry[entity_reference]

        # check if its an xref
        if type(ref_data) is dict:
            valid_id = _validate_id_in_nn_entry(
                entity=entity_reference,
                parent_entry=entry,
                parent_entity=parent_entity,
                nn=nn,
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
                        nn=nn,
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
                        entity=entity_reference, nn=nn, bbmri_id=ref
                    ):
                        validations.append(
                            {
                                "entity_reference": entity_reference,
                                "ref_id": ref,
                                "valid": False,
                            }
                        )
    return validations
