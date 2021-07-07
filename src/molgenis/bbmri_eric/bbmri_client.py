from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

from molgenis.bbmri_eric._model import NodeData, Table
from molgenis.bbmri_eric.nodes import Node
from molgenis.client import MolgenisRequestError, Session


@dataclass(frozen=True)
class ReferenceAttributeNames:
    xrefs: List[str]
    mrefs: List[str]
    categoricals: List[str]
    categorical_mrefs: List[str]
    one_to_manys: List[str]


class ReferenceType(Enum):
    XREF = "XREF"
    MREF = "MREF"
    CATEGORICAL = "CATEGORICAL"
    CATEGORICAL_MREF = "CATEGORICAL_MREF"
    ONE_TO_MANY = "ONE_TO_MANY"


class BbmriSession(Session):
    """
    BBMRI Session Class, which extends the molgenis py client Session class
    """

    def __init__(self, url: str, token: Optional[str] = None):
        super().__init__(url, token)
        self.url = url

    def get_node_staging_data(self, node: Node) -> NodeData:
        persons = Table(
            simple_name="persons",
            full_name=node.persons_staging_id,
            rows=self.get_all_rows(node.persons_staging_id),
        )

        networks = Table(
            simple_name="networks",
            full_name=node.networks_staging_id,
            rows=self.get_all_rows(node.networks_staging_id),
        )

        biobanks = Table(
            simple_name="biobanks",
            full_name=node.biobanks_staging_id,
            rows=self.get_all_rows(node.biobanks_staging_id),
        )

        collections = Table(
            simple_name="collections",
            full_name=node.persons_staging_id,
            rows=self.get_all_rows(node.collections_staging_id),
        )

        return NodeData(
            node=node,
            persons=persons,
            networks=networks,
            biobanks=biobanks,
            collections=collections,
        )

    def remove_rows(self, entity, ids):
        if len(ids) > 0:
            try:
                self.delete_list(entity, ids)
            except MolgenisRequestError as exception:
                raise ValueError(exception)

    def get_all_rows(self, entity):
        data = []
        while True:
            if len(data) == 0:
                # api can handle 10.000 max per request
                data = self.get(entity=entity, num=10000, start=len(data))
                if len(data) == 0:
                    break  # if the table is empty
            else:
                newdata = self.get(entity=entity, num=10000, start=len(data))
                if len(newdata) > 0:
                    data.extend(data)
                else:
                    break

        return data

    def get_reference_attribute_names(self, id_: str) -> ReferenceAttributeNames:
        """
        Gets the names of all reference attributes of an entity type

        Parameters:
            id_ (str): the id of the entity type
        """
        attrs = self.get_entity_meta_data(id_)["attributes"]

        result = defaultdict(list)
        for name, attr in attrs.items():
            try:
                type_ = ReferenceType[attr["fieldType"]]
                result[type_].append(name)
            except KeyError:
                pass

        return ReferenceAttributeNames(
            xrefs=result.get(ReferenceType.XREF, []),
            mrefs=result.get(ReferenceType.MREF, []),
            categoricals=result.get(ReferenceType.CATEGORICAL, []),
            categorical_mrefs=result.get(ReferenceType.CATEGORICAL_MREF, []),
            one_to_manys=result.get(ReferenceType.ONE_TO_MANY, []),
        )

    def bulk_add_all(self, entity, data):
        if len(data) == 0:
            return

        max_update_count = 1000

        if len(data) <= max_update_count:
            try:
                self.add_all(entity=entity, entities=data)
                return
            except MolgenisRequestError as exception:
                raise ValueError(exception)

        number_of_cycles = int(len(data) / max_update_count)

        try:
            for cycle in range(number_of_cycles):
                next_batch_start = int(cycle * max_update_count)
                next_batch_stop = int(max_update_count + cycle * max_update_count)
                items_to_add = data[next_batch_start:next_batch_stop]
                self.add_all(entity=entity, entities=items_to_add)
        except MolgenisRequestError as exception:
            raise ValueError(exception)
