from typing import List

from molgenis.bbmri_eric.nodes import Node


def get_all_ids(data):
    if len(data) == 0:
        return []

    return [item["id"] for item in data]


def transform_to_molgenis_upload_format(data, one_to_manys: List[str]):
    upload_format = []
    for item in data:
        new_item = item
        del new_item["_href"]
        for one_to_many in one_to_manys:
            del new_item[one_to_many]
        for key in new_item:
            if type(new_item[key]) is dict:
                ref = new_item[key]["id"]
                new_item[key] = ref
            elif type(new_item[key]) is list:
                if len(new_item[key]) > 0:
                    # get id for each new_item in list
                    mref = [ref["id"] for ref in new_item[key]]
                    new_item[key] = mref
        upload_format.append(new_item)
    return upload_format


def get_all_ref_ids_by_entity(
    entry: dict,
    possible_entity_references: list,
) -> dict:

    ref_ids_by_entity = {}

    for entity_reference in possible_entity_references:
        if entity_reference not in ref_ids_by_entity:
            ref_ids_by_entity[entity_reference] = []

        ref_data = entry[entity_reference]

        # check if its an xref
        if type(ref_data) is dict:
            ref_ids_by_entity[entity_reference].append(ref_data["id"])
        else:
            for ref in ref_data:
                if type(ref) is dict:
                    ref_ids_by_entity[entity_reference].append(ref["id"])
                else:
                    ref_ids_by_entity[entity_reference].append(ref)

    return ref_ids_by_entity


def filter_national_node_data(data: List[dict], node: Node) -> List[dict]:
    """
    Filters data from an entity based on national node code in an Id
    """
    national_node_signature = f":{node.code}_"
    data_from_national_node = [
        row for row in data if national_node_signature in row["id"]
    ]
    return data_from_national_node
