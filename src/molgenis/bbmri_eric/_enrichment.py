from molgenis.bbmri_eric._model import NodeData


def enrich_node(node_data: NodeData):
    """
    Takes the data of a Node and sets the commercial_use boolean of all collections
    based on a set of criteria.

    :param NodeData node_data: data of a node to enrich
    """

    for collection in node_data.collections.rows:

        def is_true(row: dict, attr: str):
            return attr in row and row[attr] is True

        biobank_id = collection["biobank"]
        biobank = node_data.biobanks.rows_by_id[biobank_id]

        collection["commercial_use"] = (
            is_true(biobank, "collaboration_commercial")
            and is_true(collection, "collaboration_commercial")
            and is_true(collection, "sample_access_fee")
            and is_true(collection, "image_access_fee")
            and is_true(collection, "data_access_fee")
        )
